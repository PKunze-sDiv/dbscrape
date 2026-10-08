######################################################################################################################
################################################### Einstellungen ####################################################
######################################################################################################################



script_version <- 0.7



######################################################################################################################
################################################## Datenspeicherung ##################################################
######################################################################################################################



#' Interne Funktion: Stellt eine transiente Verbindung mit Retry-Logik her
#' @keywords internal
scrp_connect <- function(sc) {
    pass <- sc$password
    
    # Wenn im Client kein Passwort hinterlegt ist, aber eine Env-Var definiert ist, dort suchen
    if (is.null(pass) && !is.null(sc$env_var) && Sys.getenv(sc$env_var) != "") {
        pass <- Sys.getenv(sc$env_var)
    }
    
    # Verbindungsparameter zusammenbauen
    args <- c(
        list(drv = sc$driver),
        if (!is.null(sc$dbname)) list(dbname = sc$dbname),
        if (!is.null(sc$host)) list(host = sc$host),
        if (!is.null(sc$port)) list(port = sc$port),
        if (!is.null(sc$user)) list(user = sc$user),
        if (!is.null(pass)) list(password = pass),
        sc$extra_args
    )
    
    # Retry-Wartezeiten: 10s, 1 Min, 5 Min, 20 Min
    backoff_times <- c(10, 60, 300, 1200)
    success <- FALSE
    attempt <- 0
    con <- NULL
    
    while (!success && attempt <= length(backoff_times)) {
        tryCatch({
            con <- do.call(DBI::dbConnect, args)
            success <- TRUE
        }, error = function(e) {
            attempt <<- attempt + 1
            if (attempt <= length(backoff_times)) {
                wait_sec <- backoff_times[attempt]
                warning(sprintf("Datenbankverbindung fehlgeschlagen (Versuch %d). Nächster Versuch in %d Sekunden. Fehler: %s", 
                                attempt, wait_sec, e$message))
                Sys.sleep(wait_sec)
            } else {
                stop(sprintf("Konnte nach mehreren Versuchen keine Verbindung zur Datenbank herstellen. Letzter Fehler: %s", e$message))
            }
        })
    }
    
    return(con)
}



#' Scraper Client erstellen (S3-Klasse): Hält die Verbindungsparameter und instanziert Logging
#'
#' @param driver Ein DBI-Treiber-Objekt (z.B. RSQLite::SQLite(), RPostgres::Postgres(), etc.)
#' @param dbname Name der Datenbank oder Dateipfad (bei SQLite)
#' @param host Server-Host (optional bei Remote-DBs)
#' @param port Port (optional)
#' @param user Benutzername (optional)
#' @param password Direktes Passwort (optional)
#' @param env_var Name der Umgebungsvariable für das Passwort (Standard: "DB_PASSWORD")
#' @param log_table_name Name der Logging-Tabelle in der DB
#' @param use_fake_browser Logisch. Ob chromote verwendet werden soll.
#' @param ... Weitere treiberspezifische Parameter
#'
#' @return Ein S3-Scraper-Client-Objekt
#'
#' @export
scrp_client <- function(
    driver, 
    dbname = NULL, 
    host = NULL, 
    port = NULL, 
    user = NULL, 
    password = NULL, 
    env_var = "DB_PASSWORD",
    log_table_name = "log", 
    use_fake_browser = FALSE,
    ...
) {

    # Prüfen, ob es sich um einen SQLite-Treiber handelt
    is_sqlite <- inherits(driver, "SQLiteDriver")

    # Passwort-Auflösung (nur wenn es KEINE SQLite-Datenbank ist!)
    resolved_pass <- NULL
    if (!is_sqlite) {
        if (!is.null(env_var) && Sys.getenv(env_var) != "") {
            resolved_pass <- Sys.getenv(env_var)
        } else if (!is.null(password)) {
            resolved_pass <- password
        } else if (interactive()) {
            message("Kein Datenbank-Passwort in Umgebungsvariablen gefunden.")
            resolved_pass <- readline(prompt = "Bitte Datenbank-Passwort eingeben: ")
        }
    }
    
    # Temporäres Client-Objekt vorab zusammenbauen, damit scrp_connect darauf zugreifen kann
    temp_sc <- structure(
        list(
            driver     = driver,
            dbname     = dbname,
            host       = host,
            port       = port,
            user       = user,
            password   = resolved_pass,
            env_var    = env_var,
            extra_args = list(...),
            log_table_name = log_table_name,
            use_fake_browser = use_fake_browser
        ), 
        class = "scrp_client"
    )

    # Verbindung kurz für die Initialisierung öffnen und direkt wieder schließen
    con <- scrp_connect(temp_sc)
    on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

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
    
    return(temp_sc)
}



#' API-Schnittstelle: Status/Log in die DB schreiben
#' 
#' @param con Connection zur Datenbank.
#' @param url Die URL der betreffenden Seite
#' @param status Der sinngemäße Status des Scraping-Versuchs (z.B. "success", "not found", "unavailable", "blocked", "redirected", "missing data")
#' @param http_status Der Status der http request
scrp_log_status <- function(con, url, status, http_status = NA_character_, log_table_name) {
    
    current_time <- format(Sys.time(), tz = "UTC", format = "%Y-%m-%d %H:%M:%SZ")
    
    # Prüfen, ob die URL bereits in der Log-Tabelle existiert
    query <- sprintf("SELECT 1 FROM %s WHERE url = ? LIMIT 1", log_table_name)
    url_exists <- nrow(DBI::dbGetQuery(con, query, params = list(url))) > 0
    
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
            log_table_name,
            if (status == "success") 0L else 1L,
            if (status == "success") 1L else 0L
        )
        DBI::dbExecute(con, query, params = list(current_time, status, http_status, url))
    
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
        DBI::dbWriteTable(con, log_table_name, new_row, append = TRUE)

    }
    
}



#' Interne Schnittstelle: Daten in die Datenbank schreiben
#' 
#' @param con Connection zur Datenbank.
#' @param data_table Das Tibble mit den zu schreibenden Daten.
#' @param target_table Charakter. Name der Ziel-Tabelle.
#' @param key_columns Charakter-Vektor oder \code{NULL}. Die Primärschlüssel für den Abgleich. 
#'     Falls \code{NULL}, werden die Daten rein chronologisch angehängt (Append).
#' 
#' @return Logisch. TRUE bei Erfolg, FALSE wenn keine Daten übergeben wurden.
#' @keywords internal
scrp_write_db <- function(con, data_table, target_table, key_columns = NULL) {
    
    if (is.null(data_table) || nrow(data_table) == 0) {
        return(FALSE)
    }
    
    table_existed <- DBI::dbExistsTable(con, target_table)
    
    if (!table_existed) {
        # Tabelle existiert noch nicht -> Struktur leeren und neu anlegen
        DBI::dbWriteTable(con, target_table, data_table |> dplyr::slice(0))
    } else {
        # Tabelle existiert -> Prüfen, ob neue Spalten dynamisch hinzugefügt werden müssen
        existing_cols <- DBI::dbListFields(con, target_table)
        new_cols <- setdiff(names(data_table), existing_cols)
        
        if (length(new_cols) > 0) {
            for (col in new_cols) {
                query <- paste0("ALTER TABLE ", target_table, " ADD COLUMN ", col, " TEXT;")
                DBI::dbExecute(con, query)
                message(paste("Datenbank erweitert: Spalte", col, "zu Tabelle", target_table, "hinzugefügt."))
            }
        }
    }
    
    # WEICHE: Append (keine Keys) vs. Upsert (mit Keys)
    if (is.null(key_columns) || length(key_columns) == 0) {
        # Reines Anhängen (Append) für Auto-ID-Tabellen / Duplikate
        DBI::dbWriteTable(con, target_table, data_table, append = TRUE, row.names = FALSE)
    } else {
        # Upsert-Logik für eindeutige Schlüssel
        db_tbl <- dplyr::tbl(con, target_table)
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
            is_empty <- nrow(DBI::dbGetQuery(con, check_query)) == 0
            
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



#' Führt den HTTP-Request oder Browser-Abruf für eine einzelne URL aus
#'
#' @description
#' Interne Engine zum Abrufen einer URL (entweder über \code{httr2} oder \code{chromote}),
#' optionaler Validierung und anschließender Extraktion. Hat keine direkten Datenbank-Side-Effects.
#'
#' @param url Character. Die abzurufende URL.
#' @param validate_fn Funktion. Optionaler Validierungs-Callback, der das HTML-Dokument prüft.
#' @param extract_fn Funktion. Optionaler Extraktions-Callback, der Daten aus dem HTML-Dokument extrahiert.
#' @param fail_on_redirect Logical. Ob ein Redirect als Fehler gewertet werden soll.
#' @param use_fake_browser Logical. Ob ein Headless-Browser via Chromote verwendet werden soll.
#' 
#' @return Eine Liste mit den Elementen \code{status}, \code{http_status} und \code{data}.
#' @keywords internal
scrp_execute <- function(
    url,
    validate_fn = NULL,
    extract_fn = NULL,
    fail_on_redirect = FALSE,
    use_fake_browser = FALSE
) {
    
    # 1. Fetch-Logik: Entweder über Fake-Browser (Chromote) oder klassisch via httr2
    fetch_result <- if (use_fake_browser) {      
        if (Sys.getenv("CHROMOTE_CHROME") == "") scrp_setup_browser()

        tryCatch({
            b <- chromote::ChromoteSession$new()
            b$Page$navigate(url)
            Sys.sleep(stats::runif(1, 4, 10))
            html_string <- b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
            html <- rvest::read_html(html_string)
            b$close()
            list(html = html, http_status = "200")
        }, error = function(e) {
            NULL
        })
        
    } else {
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
            
            # Prüfen auf unerwünschte Weiterleitungen
            final_url <- httr2::resp_url(resp)
            if (fail_on_redirect && scrp_has_redirect(url, final_url)) {
                return(list(status = "redirected", http_status = http_status, data = NULL))
            }
            
            list(html = httr2::resp_body_html(resp), http_status = http_status)
            
        }, httr2_http = function(cnd) {
            http_status <- as.character(httr2::resp_status(cnd$resp))
            status_string <- dplyr::case_when(
                http_status == "404" ~ "not found",
                http_status %in% c("403", "429") ~ "blocked",
                TRUE ~ "unavailable"
            )
            return(list(status = status_string, http_status = http_status, data = NULL))
            
        }, error = function(e) {
            return(list(status = "unavailable", http_status = "CONNECTION_ERROR", data = NULL))
        })
    }
    
    # Fehlerbehandlung beim Fetch-Vorgang
    if (is.null(fetch_result) || !is.null(fetch_result$status)) {
        if (!is.null(fetch_result$status)) return(fetch_result)
        return(list(status = "unavailable", http_status = "BROWSER_ERROR", data = NULL))
    }
    
    html        <- fetch_result$html
    http_status <- fetch_result$http_status
    
    # 2. Validierung des HTML-Inhalts
    if (!is.null(validate_fn)) {
        if (!validate_fn(html)) {
            return(list(status = "missing data", http_status = http_status, data = NULL))
        }
    }
    
    # 3. Datenextraktion
    if (!is.null(extract_fn)) {
        extracted_data <- extract_fn(html)
        is_empty <- is.null(extracted_data) || 
            length(extracted_data) == 0 || 
            all(sapply(extracted_data, function(df) is.data.frame(df) && nrow(df) == 0))
        
        if (is_empty) {
            return(list(status = "missing data", http_status = http_status, data = NULL))
        }
        
        return(list(status = "success", http_status = http_status, data = extracted_data))
    }
    
    return(list(status = "success", http_status = http_status, data = html))
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
    
    # Tabelle in DB anlegen
    con <- scrp_connect(sc)
    DBI::dbCreateTable(con, table_name, fields)
    DBI::dbDisconnect(con)
    
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
    con <- scrp_connect(sc)
    data <- dplyr::tbl(con, table_name) |> dplyr::collect()
    DBI::dbDisconnect(con)
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
#' @param batch_size Positive Ganzzahl. Anzahl an URLs, die abgearbeitet werden, bevor
#'     Daten in die Datenbank geschrieben werden (Standard: 1). Bei NULL werden alle 
#'     Daten im Arbeitsspeicher gesammelt und erst nach dem Scraping aller URLs in die
#'     Datenbank geschrieben. Dient der Ausbalancierung von Arbeitsspeichernutzung und
#'     Datenbankzugriffen.
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
    wait_max_seconds = 300,
    batch_size = 1
) {
    # 1. Validierung von batch_size
    if (!is.null(batch_size)) {
        if (!is.numeric(batch_size) || length(batch_size) != 1 || is.na(batch_size) || batch_size <= 0) {
            stop("Fehler: 'batch_size' muss eine positive ganze Zahl (z.B. 1, 50) oder NULL (gebündelt am Ende) sein.")
        }
        batch_size <- as.integer(batch_size)
    }

    # 2. Validierung der target_tables
    if (!is.list(target_tables) || is.null(names(target_tables)) || any(names(target_tables) == "")) {
        stop("Fehler: 'target_tables' muss eine benannte Liste von Tabellen-Konfigurationen sein.")
    }
    
    # 3. Detail-Validierung der Tabellen-Konfigurationen
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
    
    # 4. Zusammenbau
    job_args <- list(
        input_table           = input_table,
        input_url_column      = input_url_column,
        target_tables         = target_tables,
        validate_fn           = validate_fn,
        extract_fn            = extract_fn,
        wait_min_seconds      = wait_min_seconds,
        wait_max_seconds      = wait_max_seconds,
        batch_size            = batch_size
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
    
    # AUTOMATISMUS: Falls input NULL, aus DB laden
    if (is.null(job$input_table)) {
        input_table_name <- names(job$target_tables)[1]
        message(sprintf("Lade Input automatisch aus Tabelle '%s'...", input_table_name))
        
        con <- scrp_connect(sc)
        if (!DBI::dbExistsTable(con, input_table_name)) {
            DBI::dbDisconnect(con)
            stop(paste("Fehler: Tabelle", input_table_name, "existiert nicht in Datenbank."))
        }
        
        required_cols <- unique(c(job$input_url_column, all_inherited))
        job$input_table <- con |> 
            dplyr::tbl(input_table_name) |> 
            dplyr::select(dplyr::all_of(required_cols)) |> 
            dplyr::collect()
        
        DBI::dbDisconnect(con)
    }
    
    # VALIDIERUNG DES INPUTS
    if (!is.data.frame(job$input_table)) {
        stop("Fehler: 'input_table' muss ein Data-Frame oder Tibble sein.")
    }
    
    if (!job$input_url_column %in% names(job$input_table)) {
        stop(paste("Fehler: Die URL-Spalte '", job$input_url_column, "' wurde in der 'input_table' nicht gefunden."))
    }
    
    if (length(all_inherited) > 0) {
        missing_keys <- setdiff(all_inherited, names(job$input_table))
        if (length(missing_keys) > 0) {
            stop(paste(
                "Fehler: Die folgenden in der Job-Konfiguration angeforderten Spalten wurden in der 'input_table' nicht gefunden:", 
                paste(missing_keys, collapse = ", ")
            ))
        }
    }
    
    # BATCHING-STRUKTUR: Input-Daten in Batches aufteilen
    total_rows <- nrow(job$input_table)
    b_size <- if (is.null(job$batch_size)) total_rows else job$batch_size
    batch_indices <- split(seq_len(total_rows), ceiling(seq_len(total_rows) / b_size))
    
    message(sprintf("Starte Job: %d URLs aufgeteilt in %d Batch(es).", total_rows, length(batch_indices)))
    
    # SCHLEIFE ÜBER DIE BATCHES
    for (b_idx in seq_along(batch_indices)) {
        rows_in_batch <- batch_indices[[b_idx]]
        batch_data <- job$input_table[rows_in_batch, , drop = FALSE]
        
        message(sprintf("\n--- Verarbeite Batch %d/%d (%d URLs) ---", b_idx, length(batch_indices), nrow(batch_data)))
        
        scrp_run_batch(sc = sc, job = job, batch_data = batch_data)
    }
    
    return(invisible(TRUE))
}



#' Führt einen Scraping-Batch für eine Liste von URLs aus
#'
#' @description
#' Iteriert über alle URLs eines Batches, ruft die Engine (\code{scrp_execute}) auf,
#' puffert sowohl die extrahierten Datentabellen als auch die Status-Logs im Arbeitsspeicher
#' und schreibt am Ende des Batches alles in einer einmaligen Datenbank-Transaktion weg.
#'
#' @param sc Liste. Die Client-Konfiguration.
#' @param job Liste. Die Job-Definition inklusive Zieltabellen und Parametern.
#' @param batch_data Dataframe. Die Daten des aktuellen Batches (inklusive URL-Spalte).
#' 
#' @return Unsichtbar \code{TRUE} bei erfolgreichem Durchlauf.
#' @keywords internal
scrp_run_batch <- function(sc, job, batch_data) {
    urls <- batch_data[[job$input_url_column]]
    expected_tables <- names(job$target_tables)
    
    # Puffer-Strukturen für diesen Batch im Arbeitsspeicher
    data_buffers <- stats::setNames(replicate(length(expected_tables), list(), simplify = FALSE), expected_tables)
    log_buffer <- list()
    
    # SCRAPING SCHLEIFE (läuft komplett ohne offene DB-Verbindung im Hintergrund)
    for (i in seq_along(urls)) {
        url <- urls[i]
        
        message(sprintf("Verarbeite URL %d/%d: %s", i, length(urls), url))
        
        # Request ausführen (liefert Daten und Status zurück, ohne DB-Side-Effects)
        res <- scrp_execute(
            url              = url, 
            validate_fn      = job$validate_fn, 
            extract_fn       = job$extract_fn,
            use_fake_browser = sc$use_fake_browser
        )
        
        # Status-Log für den Batch-Puffer vormerken
        log_buffer <- append(log_buffer, list(list(
            url         = url, 
            status      = res$status, 
            http_status = res$http_status
        )))
        
        message(sprintf(" -> Status: %s (HTTP: %s)", res$status, res$http_status))
        
        # Wenn der Scraping-Vorgang erfolgreich war, Daten weiterverarbeiten
        if (res$status == "success" && !is.null(res$data)) {
            extracted_data <- res$data
            actual_tables <- names(extracted_data)
            
            if (length(setdiff(expected_tables, actual_tables)) > 0) stop("Fehler: Extractor lieferte zu wenige Tabellen.")
            if (length(setdiff(actual_tables, expected_tables)) > 0) stop("Fehler: Extractor lieferte unbekannte Tabellen.")
            
            # Speichern der Tabellen im lokalen Puffer
            for (table_name in expected_tables) {
                table_config <- job$target_tables[[table_name]]
                table_keys   <- table_config$target_key_columns
                raw_data     <- extracted_data[[table_name]]
                
                if (is.null(raw_data) || (is.data.frame(raw_data) && nrow(raw_data) == 0)) next
                
                # 1. Vererbung von Spalten aus der input_table
                if (!is.null(table_config$inherit_input_columns)) {
                    for (col in table_config$inherit_input_columns) {
                        raw_data[[col]] <- batch_data[[col]][i]
                    }
                }
                
                # 2. Validierung der Schlüssel (nur bei Upsert-Tabellen)
                if (!is.null(table_keys)) {
                    missing_keys <- setdiff(table_keys, names(raw_data))
                    if (length(missing_keys) > 0) {
                        stop(paste("Fehler: In den extrahierten Daten für Tabelle '", table_name, 
                                   "' fehlen die definierten Schlüssel:", paste(missing_keys, collapse = ", ")))
                    }
                }
                
                data_buffers[[table_name]] <- append(data_buffers[[table_name]], list(raw_data))
            }
        }
        
        # Wartezeit zwischen den Requests einhalten (falls nicht die letzte URL)
        if (i < length(urls)) {
            Sys.sleep(stats::runif(1, job$wait_min_seconds, job$wait_max_seconds))
        }
    }
    
    # FLUSH: Verbindung wird für diesen Batch einmalig geöffnet und am Ende wieder geschlossen
    message(sprintf(">>> FLUSH: Schreibe Batch-Ergebnisse in die Datenbank (%d URLs)...", length(urls)))
    con <- scrp_connect(sc)
    on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
    
    # TRANSAKTION STARTEN: Garantiert Konsistenz (Entweder alles oder nichts)
    DBI::dbBegin(con)
    
    success <- tryCatch({
        
        # 1. Gesammelte Datentabellen in die DB schreiben
        for (table_name in expected_tables) {
            if (length(data_buffers[[table_name]]) > 0) {
                combined_data <- dplyr::bind_rows(data_buffers[[table_name]])
                table_config <- job$target_tables[[table_name]]
                
                scrp_write_db(
                    con          = con, 
                    data_table   = combined_data, 
                    target_table = table_name, 
                    key_columns  = table_config$target_key_columns
                )
            }
        }
        
        # 2. Gesammelte Logs gebatcht in die DB schreiben
        if (length(log_buffer) > 0) {
            for (log_entry in log_buffer) {
                scrp_log_status(
                    con            = con, 
                    url            = log_entry$url, 
                    status         = log_entry$status, 
                    http_status    = log_entry$http_status, 
                    log_table_name = sc$log_table_name
                )
            }
        }
        
        # Wenn alles geklappt hat, Transaktion bestätigen
        DBI::dbCommit(con)
        TRUE
        
    }, error = function(e) {
        # Bei jedem Fehler: Transaktion komplett rückgängig machen!
        DBI::dbRollback(con)
        warning(paste("Fehler beim Flush-Vorgang. Transaktion wurde zurückgerollt:", e$message))
        FALSE
    })
    
    if (!success) {
        stop("Datenbank konnte nicht beschrieben werden. Programm wird abgebrochen.")
    }
    
    return(invisible(TRUE))
}