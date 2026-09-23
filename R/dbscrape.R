######################################################################################################################
################################################### Einstellungen ####################################################
######################################################################################################################



script_version <- 0.6



######################################################################################################################
################################################## Datenspeicherung ##################################################
######################################################################################################################



#' Scraper Client erstellen (S3-Klasse): Ermöglicht Datenbankzugriff und Logging
#'
#' @param con Ein bestehendes DBI-Verbindungsobjekt (z.B. zu SQLite)
#' @param log_table_name Name der Logging-Tabelle in der DB
#'
#' @return Ein S3-Objekt mit Datenbankverbindung und Kennung der Logging-Tabelle
#'
#' @export
scrp_client <- function(con, log_table_name = "log", use_fake_browser = FALSE) {

    # Settings-Tabelle initialisieren, falls nicht existent
    if (!DBI::dbExistsTable(con, "settings")) {
        DBI::dbExecute(con, "
            CREATE TABLE settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_tag TEXT
            );
        ")
        DBI::dbExecute(con, sprintf(
            "INSERT INTO settings (version_tag) VALUES ('%s');",
            as.character(script_version)
        ))
    } else {
        # Verion checken
        settings_data <- DBI::dbGetQuery(con, "SELECT * FROM settings") |> tibble::as_tibble()
        if (nrow(settings_data) != 1) {
            stop("Database settings table is corrupt.")
        }
        if (settings_data$version_tag != script_version) {
            stop(paste0("Version conflict between script (V", script_version, ") and database (V", settings_data$version_tag, ")"))
        }
    }

    # Logging-Tabelle initialisieren, falls nicht existent
    if (!DBI::dbExistsTable(con, log_table_name)) {
        DBI::dbExecute(con, sprintf("
            CREATE TABLE %s (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                last_scrape_time TEXT,
                last_scrape_status TEXT,
                last_scrape_http_status TEXT,
                failed_attempts_count INTEGER DEFAULT 0,
                successful_attempts_count INTEGER DEFAULT 0,
                in_use INTEGER DEFAULT 0
            );",
            DBI::dbQuoteIdentifier(con, log_table_name)
        ))
    }
    
    # S3-Objekt strukturieren
    structure(
        list(
            con = con,
            log_table_name = log_table_name,
            use_fake_browser = use_fake_browser
        ), 
        class = "scrp_client"
    )

}



#' API-Schnittstelle: Status/Log in die DB schreiben
#' 
#' @param sc Das S3-Scraper-Client-Objekt
#' @param url Die URL der betreffenden Seite
#' @param status Der sinngemäße Status des Scraping-Versuchs (z.B. "success", "not found", "unavailable", "blocked", "redirected", "missing data")
#' @param http_status Der Status der http request
scrp_log_status <- function(sc, url, status, http_status = NA_character_) {
    
    current_time <- format(Sys.time(), tz = "UTC", format = "%Y-%m-%d %H:%M:%SZ")
    
    # Prüfen, ob die URL bereits in der Log-Tabelle existiert
    query <- sprintf("SELECT 1 FROM %s WHERE url = ? LIMIT 1", sc$log_table_name)
    url_exists <- nrow(DBI::dbGetQuery(sc$con, query, params = list(url))) > 0
    
    if (url_exists) {   # Update-Logik: Bestehenden Eintrag aktualisieren

        increment <- ifelse(status != "success", "failed_attempts_count + 1", "0")

        query <- sprintf("
            UPDATE %s 
            SET last_scrape_time = ?, 
                last_scrape_status = ?, 
                last_scrape_http_status = ?, 
                failed_attempts_count = failed_attempts_count + %d,
                successful_attempts_count = successful_attempts_count + %d,
                in_use = 0
            WHERE url = ?", 
            sc$log_table_name,
            if (status == "success") 0L else 1L,
            if (status == "success") 1L else 0L
        )
        DBI::dbExecute(sc$con, query, params = list(current_time, status, http_status, url))
    
    } else {            # Insert-Logik für neue URLs via dplyr::rows_insert

        new_row <- tibble::tibble(
            url = url,
            last_scrape_time = current_time,
            last_scrape_status = status,
            last_scrape_http_status = http_status,
            failed_attempts_count = if (status == "success") 0L else 1L,
            successful_attempts_count = if (status == "success") 1L else 0L,
            in_use = FALSE
        )
        DBI::dbWriteTable(sc$con, sc$log_table_name, new_row, append = TRUE)

    }
    
    # Zusätzliche Ausgabe in die R-Konsole zur Live-Kontrolle
    message(sprintf("[%s] [%s] HTTP: %s", status, url, http_status))

}



#' Interne Schnittstelle: Daten in die Datenbank schreiben
#' 
#' @param sc Das S3-Scraper-Client-Objekt.
#' @param data_table Das Tibble mit den zu schreibenden Daten.
#' @param target_table Charakter. Name der Ziel-Tabelle.
#' @param key_columns Charakter-Vektor oder \خاذ{NULL}. Die Primärschlüssel für den Abgleich. 
#'     Falls \خاذ{NULL}, werden die Daten rein chronologisch angehängt (Append).
#' 
#' @return Logisch. TRUE bei Erfolg, FALSE wenn keine Daten übergeben wurden.
#' @keywords internal
scrp_write_db <- function(sc, data_table, target_table, key_columns = NULL) {
    
    if (is.null(data_table) || nrow(data_table) == 0) {
        return(FALSE)
    }
    
    table_existed <- DBI::dbExistsTable(sc$con, target_table)
    
    if (!table_existed) {
        # Tabelle existiert noch nicht -> Struktur leeren und neu anlegen
        DBI::dbWriteTable(sc$con, target_table, data_table |> dplyr::slice(0))
    } else {
        # Tabelle existiert -> Prüfen, ob neue Spalten dynamisch hinzugefügt werden müssen
        existing_cols <- DBI::dbListFields(sc$con, target_table)
        new_cols <- setdiff(names(data_table), existing_cols)
        
        if (length(new_cols) > 0) {
            for (col in new_cols) {
                query <- paste0("ALTER TABLE ", target_table, " ADD COLUMN ", col, " TEXT;")
                DBI::dbExecute(sc$con, query)
                message(paste("Datenbank erweitert: Spalte", col, "zu Tabelle", target_table, "hinzugefügt."))
            }
        }
    }
    
    # --- WEICHE: Append (keine Keys) vs. Upsert (mit Keys) ---
    if (is.null(key_columns) || length(key_columns) == 0) {
        # Reines Anhängen (Append) für Auto-ID-Tabellen / Duplikate
        DBI::dbWriteTable(sc$con, target_table, data_table, append = TRUE, row.names = FALSE)
    } else {
        # Upsert-Logik für eindeutige Schlüssel
        db_tbl <- dplyr::tbl(sc$con, target_table)
        non_key_cols <- setdiff(names(data_table), key_columns)
        
        if (length(non_key_cols) == 0) {
            dplyr::rows_insert(
                x = db_tbl,
                y = data_table,
                by = key_columns,
                in_place = TRUE,
                conflict = "ignore",
                copy = TRUE
            )
        } else {
            check_query <- sprintf("SELECT 1 FROM %s LIMIT 1", target_table)
            is_empty <- nrow(DBI::dbGetQuery(sc$con, check_query)) == 0
            
            if (is_empty) {
                dplyr::rows_insert(
                    x = db_tbl,
                    y = data_table,
                    by = key_columns,
                    in_place = TRUE,
                    conflict = "ignore",
                    copy = TRUE
                )
            } else {
                dplyr::rows_upsert(
                    x = db_tbl,
                    y = data_table,
                    by = key_columns,
                    in_place = TRUE,
                    copy = TRUE
                )
            }
        }
    }
    
    return(TRUE)
}



######################################################################################################################
#################################################### Core-Scraper ####################################################
######################################################################################################################



#' Führt einen HTTP-GET Request aus und verwaltet Validierung, Extraktion sowie Status-Logging
#'
#' @description 
#' Diese Kernfunktion kapselt den gesamten Netzwerk-Traffic des Scrapers. Sie führt genau einen 
#' GET-Request pro URL aus, fängt typische HTTP- und Verbindungsfehler ab, validiert das 
#' zurückgegebene HTML über optionale Callbacks und übergibt den finalen Status an die 
#' Logging-Schnittstelle der Datenbank.
#'
#' @param sc Ein S3-Scraper-Client-Objekt (erstellt mit \code{scrp_client()}), das die DB-Verbindung hält.
#' @param url Charakter-String. Die vollständig qualifizierte Ziel-URL der Webseite.
#' @param validate_fn Eine optionale Funktion zur Inhaltsprüfung (Prädikat). Muss das HTML der Website
#'     akzeptieren und \code{TRUE} (Inhalt valide) oder \code{FALSE} (Inhalt blockiert/ungültig) zurückgeben.
#' @param extract_fn Eine optionale Funktion zur Datenextraktion. Muss das HTML der Website akzeptieren 
#'     und eine \code{benannte Liste von Tibbles/Data-Frames} (oder \code{NULL}) zurückgeben. Die Namen der 
#'     Listenelemente müssen exakt mit den in \code{target_structures} definierten Tabellennamen übereinstimmen.
#' @param fail_on_redirect Logisch. Bestimmt, ob der Prozess abgebrochen und als \code{"redirected"} 
#'     geloggt werden soll, wenn der Server die URL intern umleitet.
#'
#' @return Gibt das Ergebnis der \code{extract_fn} zurück (eine benannte Liste von Tibbles), 
#'     das rohe HTML-Objekt (falls keine \code{extract_fn} übergeben wurde) oder \code{NULL}, 
#'     falls beim Request, der Validierung oder der Extraktion ein Fehler aufgetreten ist.
#' 
#' @export
scrp_execute <- function(sc, url, validate_fn = NULL, extract_fn = NULL, fail_on_redirect = FALSE) {
    
    fetch_result <- if (sc$use_fake_browser) {      # WEG A: Ressourcen-intensive Browser-Simulation (chromote)
        
        if (Sys.getenv("CHROMOTE_CHROME") == "") scrp_setup_browser()
        #scrp_check_zombie_processes()

        tryCatch({
            b <- chromote::ChromoteSession$new()
            b$Page$navigate(url)
            
            # Zufällige Wartezeit, um menschliches Verhalten zu simulieren
            Sys.sleep(stats::runif(1, 4, 10))
            
            html_string <- b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
            html <- rvest::read_html(html_string)
            b$close()
            
            list(html = html, http_status = "200")
            
        }, error = function(e) {
            message("=== CHROMOTE FEHLER DETEKTIERT ===")
            print(e)
            message("==================================")
            scrp_log_status(sc, url, "unavailable", "BROWSER_ERROR")
            NULL
        })
        
    } else {                                        # WEG B: Ressourcenschonender HTTP-Request (httr2 / libcurl)

        browser_id <- get_random_browser_identity()

        req <- httr2::request(url) |> 
            httr2::req_user_agent(browser_id$ua) |>
            httr2::req_headers(!!!browser_id$headers) |>
            httr2::req_options(cookiefile = "", cookiejar = "") |>
            httr2::req_retry(max_tries = 1) |>
            httr2::req_timeout(10)
        
        tryCatch({
            resp <- httr2::req_perform(req)
            http_status <- as.character(httr2::resp_status(resp))
            
            # Redirect-Prüfung
            final_url <- httr2::resp_url(resp)
            if (fail_on_redirect && scrp_has_redirect(url, final_url)) {
                scrp_log_status(sc, url, "redirected", http_status)
                return(NULL)
            }
            
            list(html = httr2::resp_body_html(resp), http_status = http_status)
            
        }, httr2_http = function(cnd) {
            http_status <- as.character(httr2::resp_status(cnd$resp))
            status_string <- dplyr::case_when(
                http_status == "404" ~ "not found",
                http_status %in% c("403", "429") ~ "blocked",
                TRUE ~ "unavailable"
            )
            scrp_log_status(sc, url, status_string, http_status)
            NULL
            
        }, error = function(e) {
            scrp_log_status(sc, url, "unavailable", "CONNECTION_ERROR")
            NULL
        })
    }
    
    # Falls beim Laden (Weg A oder B) etwas schiefgelaufen ist, brechen wir hier ab
    if (is.null(fetch_result)) {
        return(NULL)
    }
    
    # Werte für die Weiterverarbeitung entpacken
    html        <- fetch_result$html
    http_status <- fetch_result$http_status
    
    # Validierung der Seite
    if (!is.null(validate_fn)) {
        is_valid <- validate_fn(html)
        if (!is_valid) {
            scrp_log_status(sc, url, "missing data", http_status)
            return(NULL)
        }
    }
    
    # Extraktion & Erfolgsprüfung
    if (!is.null(extract_fn)) {
        extracted_data <- extract_fn(html)
        
        is_empty <- is.null(extracted_data) || 
            length(extracted_data) == 0 || 
            all(sapply(extracted_data, function(df) is.data.frame(df) && nrow(df) == 0))
        
        if (is_empty) {
            scrp_log_status(sc, url, "missing data", http_status)
            return(NULL)
        }
        
        scrp_log_status(sc, url, "success", http_status)
        return(extracted_data)
    }
    
    # Fallback: Rohes HTML bei Erfolg zurückgeben, falls kein Extractor definiert ist
    scrp_log_status(sc, url, "success", http_status)
    return(html)

}



######################################################################################################################
################################################## Hilfsfunktionen ###################################################
######################################################################################################################



#' Check, ob Weiterleitung stattgefunden hat beim Aufruf einer Website
#' @param expected_url Die URL der angesteuerten Website
#' @param actual_url Die tatsächliche URL nach Aufruf der Website
scrp_has_redirect <- function(expected_url, actual_url) {

    clean_url <- function(u) {
        u |> 
            stringr::str_to_lower() |> 
            stringr::str_replace("https?://", "") |>
            stringr::str_replace("www\\.", "") |>
            stringr::str_replace("/$", "")
    }
    return(clean_url(expected_url) != clean_url(actual_url))

}



#' Generiert eine konsistente Browser-Identität samt vollständigem Header-Set
#' 
#' @description
#' Wählt basierend auf aktuellen Marktanteilen eine Browser-Identität aus und
#' generiert ein vollständig konsistentes Set an HTTP-Headern (inkl. passender
#' Sec-Ch-Ua Client Hints), um Bot-Erkennungssysteme zu umgehen.
#' 
#' @return Eine Liste mit zwei Elementen: 
#'   \item{ua}{Charakter. Der vollständige User-Agent String.}
#'   \item{headers}{Eine benannte Liste aller HTTP-Header für den Request.}
#' @keywords internal
get_random_browser_identity <- function() {
    
    # 1. Allgemeine, für alle modernen Desktop-Browser gültige Basis-Header
    base_headers <- list(
        `Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        `Accept-Language` = "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        `Cache-Control` = "max-age=0",
        `Upgrade-Insecure-Requests` = "1",
        `Sec-Fetch-Dest` = "document",
        `Sec-Fetch-Mode` = "navigate",
        `Sec-Fetch-Site` = "none",
        `Sec-Fetch-User` = "?1"
    )
    
    # 2. Spezifische Browser-Profile mit ihren jeweiligen Marktanteilen (Gewichten)
    identities <- list(
        # Chrome auf Windows (50%)
        list(
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            weight = 0.50,
            specific_headers = list(
                `Sec-Ch-Ua` = '"Not/A)Brand";v="8", "Chromium";v="142", "Google Chrome";v="142"',
                `Sec-Ch-Ua-Mobile` = "?0",
                `Sec-Ch-Ua-Platform` = '"Windows"'
            )
        ),
        # Edge auf Windows (15%)
        list(
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
            weight = 0.15,
            specific_headers = list(
                `Sec-Ch-Ua` = '"Not/A)Brand";v="8", "Chromium";v="142", "Microsoft Edge";v="142"',
                `Sec-Ch-Ua-Mobile` = "?0",
                `Sec-Ch-Ua-Platform` = '"Windows"'
            )
        ),
        # Safari auf Mac (10% - Keine Client Hints)
        list(
            ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
            weight = 0.10,
            specific_headers = list() 
        ),
        # Firefox auf Windows (10% - Keine Client Hints)
        list(
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
            weight = 0.10,
            specific_headers = list()
        ),
        # Chrome auf Mac (8%)
        list(
            ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            weight = 0.08,
            specific_headers = list(
                `Sec-Ch-Ua` = '"Not/A)Brand";v="8", "Chromium";v="142", "Google Chrome";v="142"',
                `Sec-Ch-Ua-Mobile` = "?0",
                `Sec-Ch-Ua-Platform` = '"macOS"'
            )
        ),
        # Firefox auf Linux (7% - Keine Client Hints)
        list(
            ua = "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
            weight = 0.07,
            specific_headers = list()
        )
    )
    
    # 3. Zufallsauswahl nach Gewichtung
    weights <- sapply(identities, `[[`, "weight")
    selected <- sample(identities, 1, prob = weights)[[1]]
    
    # 4. Mergen der Basis-Header mit den systemspezifischen Headern
    final_headers <- utils::modifyList(base_headers, selected$specific_headers)
    
    # 5. Sauberes, fertiges Paket zurückgeben
    return(list(
        ua = selected$ua,
        headers = final_headers
    ))
}



#' Bereitet die extrahierten Daten für den Rückgabewert des Extractors vor
#'
#' @description
#' \code{scrp_result} ist eine Helper-Funktion innerhalb der \code{extract_fn}. Sie konvertiert 
#' benannte Argumente in ein Tibble, ordnet dieses einer Ziel-Tabelle zu und verpackt 
#' es in die vom Scraper erwartete Listenstruktur. Durch ihr Design lässt sie sich 
#' nahtlos mit der R-Pipe (\code{|>}) verketten, um mehrere Tabellen gleichzeitig aufzubauen.
#'
#' @param .result Optional. Ein bereits bestehendes Resultat-Objekt (benannte Liste). 
#'     Wird bei der Verwendung mit der Pipe (\code{|>}) automatisch vom vorherigen Schritt 
#'     übergeben. Standard ist \code{NULL} für den initialen Aufruf.
#' @param .table Charakter-String. Der Name der Ziel-Datenbanktabelle.
#' @param ... Benannte Vektoren oder Listen, die die Spalten und Werte der Tabelle bilden. 
#'     Vektoren müssen die gleiche Länge haben oder auf eine gemeinsame Länge recycelbar sein.
#'
#' @return Eine benannte Liste von Tibbles, passend für den Rückgabewert der \code{extract_fn}.
#' @export
#'
#' @examples
#' # Verwendung in der extract_fn mit der R-Pipe (|>):
#' extract_fn <- function(html) {
#'     # ... Scraping-Logik ...
#'     
#'     scrp_result(.table = "pubs", pub_id = 123, score = 45) |> 
#'         scrp_result("authors", author_name = c("Anna", "Ben")) |> 
#'         scrp_result("publication_author_map", pub_id = 123, author_name = c("Anna", "Ben"))
#' }
scrp_result <- function(.result = NULL, .table, ...) {
    
    # 1. Ergebnis-Struktur initialisieren oder validieren
    if (is.null(.result)) {
        final_list <- list()
    } else {
        if (!is.list(.result) || is.data.frame(.result)) {
            stop("Fehler in scrp_result: Das übergebene '.result' ist keine gültige Ergebnis-Liste.")
        }
        final_list <- .result
    }
    
    # 2. Eingaben aus '...' validieren und in ein Tibble gießen
    dots <- list(...)
    
    if (length(dots) == 0) {
        new_table <- tibble::tibble()
    } else {
        if (is.null(names(dots)) || any(names(dots) == "")) {
            stop("Fehler in scrp_result: Alle Argumente in '...' müssen benannt sein (z.B. spalten_name = wert).")
        }
        
        new_table <- tryCatch({
            tibble::tibble(!!!dots)
        }, error = function(e) {
            stop(paste("Fehler beim Erstellen der Tabelle '", .table, 
                       "'. Die übergebenen Vektoren haben inkompatible Längen:\n", e$message))
        })
    }
    
    # 3. Das Tibble benannt in die Liste einfügen
    final_list[[.table]] <- new_table
    
    return(final_list)
}



#' Richtet den Pfad für den simulierten Browser (chromote) ein
#'
#' @param path Optionaler, spezifischer Pfad zur ausführbaren Datei des Browsers (z.B. chrome.exe oder msedge.exe).
#'             Falls NULL, sucht die Funktion nach Standardpfaden für Chrome, Edge und Brave.
#' @return Der gesetzte Pfad (unsichtbar).
#' @export
scrp_setup_browser <- function(path = NULL) {
    if (!is.null(path)) {
        if (!file.exists(path)) {
            stop(paste("Der angegebene Pfad existiert nicht:", path))
        }
        Sys.setenv(CHROMOTE_CHROME = path)
        message(paste("Browser-Pfad manuell gesetzt auf:", path))
        return(invisible(path))
    }
    
    # Standardpfade je nach Betriebssystem absuchen
    possible_paths <- c(
        # Windows
        "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
        "C:/Program Files/Google/Chrome/Application/chrome.exe",
        "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe",
        "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe",
        # macOS
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        # Linux
        "/usr/bin/microsoft-edge",
        "/usr/bin/google-chrome",
        "/usr/bin/brave-browser",
        "/usr/bin/chromium"
    )
    
    found_paths <- possible_paths[file.exists(possible_paths)]
    
    if (length(found_paths) > 0) {
        selected_path <- found_paths[1]
        Sys.setenv(CHROMOTE_CHROME = selected_path)
        # Argumente für Chromium-Browser optimieren
        opt_args <- c(
            "--disable-gpu",
            "--no-sandbox",
            "--disable-dev-shm-usage"
        )
        chromote::set_chrome_args(opt_args)    
        # Dem Nutzer kurz rückmelden, welcher Browser gewählt wurde
        message(paste("Automatisch gefunden und konfiguriert:", selected_path))
        return(invisible(selected_path))
    } else {
        stop("Es wurde kein Chromium-basierter Browser (Edge, Chrome, Brave, Chromium) an den Standardorten gefunden.\nBitte gib den Pfad manuell an: scrp_setup_browser('PFAD/ZUR/DATEI.exe')")
    }
}



#' Prüft auf blockierende Browser-Hintergrundprozesse und bietet an, diese zu beenden
#'
#' @keywords internal
scrp_check_zombie_processes <- function() {
    chrome_env <- Sys.getenv("CHROMOTE_CHROME")
    
    if (chrome_env == "") {
        scrp_setup_browser()
        chrome_env <- Sys.getenv("CHROMOTE_CHROME")
    }
    
    # basename säubern und eventuelle Whitespaces entfernen
    browser_exe <- trimws(basename(chrome_env))
    # Fall generisch abfangen, falls chrome_env leer geblieben ist
    if (browser_exe == "" || is.na(browser_exe)) browser_exe <- "chrome"
    
    process_running <- FALSE
    
    if (.Platform$OS.type == "windows") {
        tasks <- tryCatch({
            system2("tasklist", args = c("/NH", "/FO", "CSV"), stdout = TRUE)
        }, error = function(e) NULL)
        
        if (!is.null(tasks)) {
            tasks_utf8 <- iconv(tasks, from = "latin1", to = "UTF-8", sub = "")
            process_running <- any(grepl(browser_exe, tasks_utf8, ignore.case = TRUE))
        }
        
    } else {
        # macOS / Linux
        tasks <- tryCatch({
            system2("pgrep", args = c("-f", browser_exe), stdout = TRUE)
        }, error = function(e) NULL)
        
        if (!is.null(tasks) && length(tasks) > 0) {
            process_running <- TRUE
        }
    }
    
    if (process_running) {
        message(paste0("\nHinweis: Es laufen bereits Hintergrundprozesse von ", browser_exe, "."))
        message("Diese können die Verbindung von chromote blockieren (Port-Konflikt).")
        
        if (interactive()) {
            answer <- readline(prompt = paste0("Möchtest du alle laufenden ", browser_exe, "-Prozesse jetzt beenden? (j/n): "))
            
            if (tolower(answer) %in% c("j", "ja", "y", "yes")) {
                message("Beende Prozesse...")
                if (.Platform$OS.type == "windows") {
                    system2("taskkill", args = c("/F", "/IM", browser_exe), stdout = FALSE, stderr = FALSE)
                } else {
                    system2("pkill", args = c("-f", browser_exe), stdout = FALSE,stderr = FALSE)
                }
                Sys.sleep(1)
            } else {
                message("Prozesse wurden nicht beendet. Falls gleich ein Port-Fehler auftritt, liegt es sehr wahrscheinlich daran.")
            }
        } else {
            warning(paste0("Laufende ", browser_exe, "-Instanzen detektiert. Im Headless-Modus kann dies zu Port-Fehlern führen."))
        }
    }
}



#' Erstellt eine neue Tabelle in der Datenbank über Spalten und Domains
#'
#' @param sc Ein \code{scrp_client}-Objekt.
#' @param table_name Name der zu erstellenden Tabelle.
#' @param col_names Charakter-Vektor mit den Spaltennamen.
#' @param domains Charakter-Vektor mit den Datentypen (z.B. "TEXT", "INTEGER", "AUTO").
#' 
#' @return Unsichtbar \code{TRUE} bei Erfolg.
#' @export
scrp_create_table <- function(sc, table_name, col_names, domains = "TEXT") {
    
    # Falls nur ein Typ übergeben wurde, für alle Spalten übernehmen
    if (length(domains) == 1) {
        domains <- rep(domains, length(col_names))
    }
    
    if (length(col_names) != length(domains)) {
        stop("Fehler: 'col_names' und 'domains' müssen exakt die gleiche Länge haben.")
    }
    
    # In Großbuchstaben konvertieren (z.B. "auto" -> "AUTO")
    domains <- toupper(domains)
    
    # Shortcut "AUTO" in den passenden SQL-Auto-Increment-Befehl übersetzen
    domains <- ifelse(domains == "AUTO", "INTEGER PRIMARY KEY AUTOINCREMENT", domains)
    
    # Benanntes Vektor-Mapping für DBI erstellen
    fields <- stats::setNames(domains, col_names)
    
    # Tabelle direkt über DBI-Treiber anlegen
    DBI::dbCreateTable(sc$con, table_name, fields)
    
    message(sprintf("Tabelle '%s' erfolgreich in der Datenbank erstellt.", table_name))
    invisible(TRUE)
}



#' Exportiert eine Tabelle aus der Scraper-DB in eine CSV-Datei
#' 
#' @param sc Das S3-Scraper-Client-Objekt
#' @param table_name Name der Tabelle in der DB
#' @param file_path Pfad zur Ziel-CSV
#' @export
scrp_export_csv <- function(sc, table_name, file_path) {
    data <- dplyr::tbl(sc$con, table_name) |> dplyr::collect()
    readr::write_csv(data, file_path) # Oder utils::write.csv, um kein readr zu erzwingen
    message(sprintf("Tabelle '%s' erfolgreich nach '%s' exportiert.", table_name, file_path))
    invisible(data)
}



######################################################################################################################
################################################## Job-Konfigurator ##################################################
######################################################################################################################



#' Erstellt eine Scraping-Job-Konfiguration
#'
#' @description
#' \code{scrp_define_job} definiert ein zentrales Konfigurationsobjekt für den Scraper. 
#' Die Funktion steuert, welche URLs verarbeitet werden sollen, wie der Inhalt extrahiert 
#' wird und in welche Datenbanktabellen die Ergebnisse geschrieben werden.
#'
#' @param input_table Optional. Ein Data-Frame oder Tibble. Enthält die 
#'     aufzurufenden Ziel-Adressen sowie optionale Metadaten. Falls \code{NULL}, 
#'     versucht der Executor, die Daten aus der ersten in \code{target_tables} 
#'     definierten Tabelle zu laden.
#' @param input_url_column Charakter. Der Name der Spalte in der \code{input_table}, welche die 
#'     aufzurufenden URLs enthält (Standard: \code{"url"}).
#' @param target_tables Eine benannte Liste, die die Ziel-Konfigurationen für die Datenbank enthält.
#'     Jedes Element entspricht einer Tabelle und kann ein Vektor mit Namen der Primärschlüssel-Spalten, 
#'     \code{NULL} (für reines Anhängen / Append-Tabellen mit Auto-ID) oder eine Liste mit folgenden Feldern sein:
#'     \describe{
#'         \item{\code{target_key_columns}}{Charakter-Vektor oder \code{NULL}. Die Spaltennamen, die den Primärschlüssel für ein Upsert bilden.}
#'         \item{\code{inherit_input_columns}}{Charakter-Vektor oder \code{NULL}. Namen von Spalten aus der \code{input_table}, die in diese spezifische Zieltabelle übernommen werden sollen (z. B. IDs oder auch die \code{"url"}).}
#'     }
#' @param validate_fn Eine optionale Funktion zur Inhalts- und Blockierungsprüfung des HTML-Dokuments.
#' @param extract_fn Eine Funktion zur Datenextraktion. Erwartet das HTML-Dokument 
#'     der Website (ein \code{xml2::xml_document}) und soll eine benannte Liste von 
#'     Tibbles oder Data-Frames zurückgeben. Die Namen der Listenelemente müssen 
#'     exakt den Tabellennamen in \code{target_tables} entsprechen. 
#'     Die Spaltennamen innerhalb der jeweiligen Data-Frames bilden die Tabellenspalten ab.
#' @param wait_max_seconds Numerisch. Maximale Wartezeit in Sekunden (Standard: 300).
#'
#' @return Ein Objekt der Klasse \code{scrp_job}.
#' @export
scrp_define_job <- function(
    input_table = NULL,
    input_url_column = "url",
    target_tables,
    validate_fn = NULL,
    extract_fn = NULL,
    wait_min_seconds = 5,
    wait_max_seconds = 300
) {
    # 1. Validierung der target_tables
    if (!is.list(target_tables) || is.null(names(target_tables)) || any(names(target_tables) == "")) {
        stop("Fehler: 'target_tables' muss eine benannte Liste von Tabellen-Konfigurationen sein.")
    }
    
    # 2. Detail-Validierung der Tabellen-Konfigurationen
    for (table_name in names(target_tables)) {
        config <- target_tables[[table_name]]
        
        # Komfort-Transformation (falls nur target_key_columns als Vektor übergeben wurden)
        if (!is.list(config)) {
            config <- list(
                target_key_columns = config,
                inherit_input_columns = NULL
            )
        }
        
        if (!"target_key_columns" %in% names(config)) {
            config$target_key_columns <- NULL
        }
        
        if (!"inherit_input_columns" %in% names(config)) {
            config$inherit_input_columns <- NULL
        }
        
        target_tables[[table_name]] <- config
    }
    
    # 3. Zusammenbau
    job_args <- list(
        input_table           = input_table,
        input_url_column      = input_url_column,
        target_tables         = target_tables,
        validate_fn           = validate_fn,
        extract_fn            = extract_fn,
        wait_min_seconds      = wait_min_seconds,
        wait_max_seconds      = wait_max_seconds
    )
    
    structure(job_args, class = "scrp_job")
}



#' High-Level Pipeline: Führt einen Scraping-Job aus
#'
#' @param sc Ein \code{scrp_client}-Objekt.
#' @param job Ein \code{scrp_job}-Objekt, erstellt durch \code{scrp_define_job}.
#'
#' @return Unsichtbar \code{TRUE} bei Erfolg.
#' @export
scrp_run_job <- function(sc, job) {
    
    if (!inherits(job, "scrp_job")) {
        stop("Fehler: Das 'job'-Argument muss von der Klasse 'scrp_job' sein.")
    }
    
    # Alle Spalten, die irgendwo vererbt werden sollen, über alle Tabellen hinweg einsammeln
    all_inherited <- unique(unlist(lapply(job$target_tables, function(cfg) cfg$inherit_input_columns)))
    
    # === AUTOMATISMUS: Input aus DB laden, falls NULL ===
    if (is.null(job$input_table)) {
        input_table_name <- names(job$target_tables)[1]
        message(sprintf("Lade Input automatisch aus Tabelle '%s'...", input_table_name))
        
        if (!DBI::dbExistsTable(sc$con, input_table_name)) {
            stop(paste("Fehler: Tabelle", input_table_name, "existiert nicht."))
        }
        
        required_cols <- unique(c(job$input_url_column, all_inherited))
        job$input_table <- sc$con |> 
            dplyr::tbl(input_table_name) |> 
            dplyr::select(dplyr::all_of(required_cols)) |> 
            dplyr::collect()
    }
    
    # === VALIDIERUNG DES INPUTS ===
    if (!is.data.frame(job$input_table)) {
        stop("Fehler: 'input_table' muss ein Data-Frame oder Tibble sein.")
    }
    
    if (!job$input_url_column %in% names(job$input_table)) {
        stop(paste("Fehler: Die URL-Spalte '", job$input_url_column, "' wurde in der 'input_table' nicht gefunden."))
    }
    
    urls <- job$input_table[[job$input_url_column]]
    
    if (length(all_inherited) > 0) {
        missing_keys <- setdiff(all_inherited, names(job$input_table))
        if (length(missing_keys) > 0) {
            stop(paste(
                "Fehler: Die folgenden in der Job-Konfiguration angeforderten Spalten wurden in der 'input_table' nicht gefunden:", 
                paste(missing_keys, collapse = ", ")
            ))
        }
    }
    
    # === SCRAPING SCHLEIFE ===
    for (i in seq_along(urls)) {
        url <- urls[i]
        
        message(sprintf("Verarbeite URL %d/%d: %s", i, length(urls), url))
        
        extracted_data <- scrp_execute(
            sc          = sc, 
            url         = url, 
            validate_fn = job$validate_fn, 
            extract_fn  = job$extract_fn
        )
        
        if (!is.null(extracted_data)) {
            
            expected_tables <- names(job$target_tables)
            actual_tables   <- names(extracted_data)
            
            if (length(setdiff(expected_tables, actual_tables)) > 0) stop("Fehler: Extractor lieferte zu wenige Tabellen.")
            if (length(setdiff(actual_tables, expected_tables)) > 0) stop("Fehler: Extractor lieferte unbekannte Tabellen.")
            
            # Speichern der Tabellen
            for (table_name in expected_tables) {
                table_config <- job$target_tables[[table_name]]
                table_keys   <- table_config$target_key_columns
                raw_data     <- extracted_data[[table_name]]
                
                if (is.null(raw_data) || (is.data.frame(raw_data) && nrow(raw_data) == 0)) next
                
                # 1. Vererbung von Spalten aus der input_table
                if (!is.null(table_config$inherit_input_columns)) {
                    for (col in table_config$inherit_input_columns) {
                        raw_data[[col]] <- job$input_table[[col]][i]
                    }
                }
                
                # 2. Validierung der Schlüssel (nur bei Upsert-Tabellen)
                if (!is.null(table_keys)) {
                    missing_keys <- setdiff(table_keys, names(raw_data))
                    if (length(missing_keys) > 0) {
                        stop(paste("Fehler: In den extrahierten Daten für Tabelle '", table_name, 
                                   "' fehlen die definierten Schlüssel ('target_key_columns'):", 
                                   paste(missing_keys, collapse = ", ")))
                    }
                }
                
                # 3. Schreiben in die DB
                scrp_write_db(
                    sc           = sc, 
                    data_table   = raw_data, 
                    target_table = table_name, 
                    key_columns  = table_keys
                )
            }
        }
        
        if (i < length(urls)) {
            wait_time <- stats::runif(1, job$wait_min_seconds, job$wait_max_seconds)
            Sys.sleep(wait_time)
        }
    }
    
    return(invisible(TRUE))
}