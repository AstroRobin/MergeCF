#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    https://shiny.posit.co/
#
library(shiny)
library(shinyjs)
library(shinybusy)

library(tidyverse)
library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(readr)
library(DT)

options(shiny.maxRequestSize = 50 * 1024^2)  # 50 MB

`%||%` <- function(a, b) if (!is.null(a)) a else b  # safety function for dealing with cases of missing variables

read_headers <- function(filepath) {
  ext <- tools::file_ext(filepath)
  if (ext == "csv") {
    colnames(readr::read_csv(filepath, n_max = 0))
  } else if (ext %in% c("xls", "xlsx")) {
    colnames(readxl::read_excel(filepath, n_max = 0))
  } else {
    stop("Unsupported file type: ", ext)
  }
}

read_data <- function(filepath) {
  ext <- tools::file_ext(filepath)
  if (ext == "csv") {
    readr::read_csv(filepath)
  } else if (ext %in% c("xls", "xlsx")) {
    readxl::read_excel(filepath)
  } else {
    stop("Unsupported file type: ", ext)
  }
}


process_dataset <- function(path, arestid_col, studyid_col, dataset_name, eventCol=NULL) {
  df0 <- read_data(path)
  #Sys.sleep(0.5)
  
  if (is.na(eventCol)) {
    df0 <- df0 %>% 
      mutate(event_name = dataset_name)  # Create event_name column filled with dataset_name
  } else {
    df0 <- df0 %>% 
      rename(event_name = eventCol)
  }
  
  df <- tryCatch ({
    df <- df0 %>%
      rename(arest_id = !!sym(arestid_col))
    
    if (arestid_col == studyid_col) {
      df <- mutate(df, study_id = arest_id)
    } else {
      df <- rename(df, study_id = !!sym(studyid_col))
    }
    
    df %>%         
      fill(arest_id) %>%
      mutate(source = dataset_name,
             arest_id = as.character(arest_id),
             study_id = as.character(study_id)) %>%
      select(arest_id, study_id, source, everything())
    
  }, error = function(e) {
    message("Error in process_dataset: ", conditionMessage(e))
    return(NULL)
  })
  
  return(df)
}


get_overlapping_columns <- function(target_cols, ..., shared_cols = character()) {
  # Get list of other dataframes from ...
  other_cols <- list(...)
  
  # Get the non-shared column names of the target dataframe
  target_cols <- setdiff(target_cols, shared_cols)
  
  overlapping_cols <- character()
  for (cols in other_cols) {
    comp_cols <- setdiff(cols, shared_cols)
    overlapping_cols <- union(overlapping_cols, intersect(target_cols, comp_cols))
  }
  
  return(overlapping_cols)
}


ui <- fluidPage(
  useShinyjs(),
  
  tags$head(
    tags$style(HTML("
    #actionButtonsContainer {
      position: fixed;
      bottom: 20px;
      right: 30px;
      z-index: 1000;
    }
    #actionButtonsContainer .btn {
      margin-left: 10px;
    }
    .disabled-block {
      opacity: 0.5;
      pointer-events: none;
    }
    .moveUp, .moveDown {
      background: none;
      border: none;
      padding: 0;
      font-size: 16px;
      cursor: pointer;
    }
    .moveUp:hover, .moveDown:hover {
      color: #007bff; /* Optional: blue hover */
    }
  "))
  ),
  
  titlePanel("Merge CF"),
  
  tabsetPanel(id = "mainTabs",
    tabPanel("Select Datasets",
      sidebarLayout(
        sidebarPanel(width=3,
          h4("Select Datasets"),
          br(),
          # File upload input
          fileInput("file", "Choose File",
                    accept = c(
                      ".csv", 
                      ".xls", 
                      ".xlsx", 
                      "application/vnd.ms-excel", 
                      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          )),
          
          # Dataset name input
          textInput("datasetName", "Enter Dataset Name"),
          # Suffix preview
          tags$div(
            style = "margin-top: -10px; font-style: italic; color: #666;",
            HTML("&emsp;suffix: "), textOutput("datasetSuffix", inline = TRUE)
          ),
          
          br(),
          # Dynamically render column selectors for arest id and study ID
          div(id = "arestIdWrapper", uiOutput("arestIdUI")),
          div(id = "studyIdWrapper", uiOutput("studyIdUI")),
          div(id = "eventColWrapper", uiOutput("eventColUI")),
          
          # Button to add dataset
          actionButton("addDataset", "Add Dataset", class = "btn-primary", disabled = TRUE)
        ),
        
        mainPanel(width=9,
          h3("Current Datasets"),
          
          # Table displaying the list of datasets with their metadata
          DTOutput("fileList"),
          
          # Displaying the preview table
          DTOutput("previewTable"),
          
          br(),
          actionButton("processDatasets", "Process Selected Datasets", class = "btn-primary"),
          br(), br(),
          # Dynamic text output to show progress
          uiOutput("progressUI"),
        )
      )
    ),
  
    # Second Tab: Another Functionality (Can be left empty or used for other functionality)
    tabPanel("Manage Events",
       sidebarLayout(
         sidebarPanel(width=3,
           h4("Manage Events"),
           br(),
           tags$div(
             title = "Please 'Process Datasets' first",
             selectizeInput("selectedDataset", "Choose dataset", choices = NULL),
           ),
           br(),
           # Save Changes button aligned to the right, just below the dropdown
           div(
             style = "text-align: right; margin-top: -10px; margin-bottom: 20px;",
             actionButton("saveEventChanges", "Save Changes", class = "btn-primary")
           )
         ),
         
         mainPanel(width=9,
           h3("Events"),
           uiOutput("eventManagerUI")
           
         )
       )
    )
  ),
  
  # Buttons container (bottom-right)
  tags$div(
    id = "actionButtonsContainer",
    actionButton("mergeDatasets", "Merge Datasets", class = "btn-primary"),
    actionButton("preview", "Preview Merged Data", class = "btn-primary")
  )
)

server <- function(input, output, session) {
  
  # Reactive value to store datasets and metadata
  rv <- reactiveValues(datasets = list(), files = character(), processed = list())
  
  # Reactively get column names (no data loaded yet)
  uploadedColnames <- reactiveVal(NULL)
  progress_message <- reactiveVal("")
  
  shinyjs::disable("preview")
  shinyjs::disable("selectedDataset")
  
  # Function to update the progress message
  update_progress <- function(message) {
    current_message <- progress_message()  # Get the current accumulated messages
    new_message <- paste0(current_message, message, "\n")  # Append the new message with a newline
    progress_message(new_message)  # Update the reactive value
    invalidateLater(500, session)  # Update the UI every 500 milliseconds
  }
  
  
  observeEvent(input$file, {
    req(input$file)
    
    tryCatch({
      cols <- read_headers(input$file$datapath)
      uploadedColnames(cols)
    }, error = function(e) {
      showNotification(paste("Error reading file:", e$message), type = "error")
      uploadedColnames(NULL)
    })
    
    # Update the default value of the dataset name text input
    file_name <- tools::file_path_sans_ext(basename(input$file$name))
    updateTextInput(session, "datasetName", value = file_name)
    
  })
  
  output$datasetSuffix <- renderText({
    req(input$datasetName)
    gsub("[^a-z0-9_]", "", tolower(input$datasetName))
  })
  
  datasetSuffix <- reactive({
    req(input$datasetName)
    gsub("[^a-z0-9_]", "", tolower(input$datasetName))
  })
  
  observe({
    if (is.null(uploadedColnames())) {
      shinyjs::addClass("arestIdWrapper", "disabled-block")
      shinyjs::addClass("studyIdWrapper", "disabled-block")
      shinyjs::addClass("eventColWrapper", "disabled-block")
    } else {
      shinyjs::removeClass("arestIdWrapper", "disabled-block")
      shinyjs::removeClass("studyIdWrapper", "disabled-block")
      shinyjs::removeClass("eventColWrapper", "disabled-block")
    }
  })
  
  output$arestIdUI <- renderUI({
    all_columns <- uploadedColnames() %||% character(0)
    arest_columns <- grep("arest", all_columns, value = TRUE, ignore.case = TRUE)
    other_columns <- setdiff(all_columns, arest_columns)
    choices <- if (length(all_columns) > 0) c(arest_columns, " ", other_columns) else ""
    
    # Render the selectizeInput UI for the arest id with a visual separator
    selectizeInput("arestId", "Select Arest ID", 
                   choices = choices, 
                   multiple = FALSE,
                   options = list(
                     "plugins" = list("remove_button"),
                     "maxItems" = 1,
                     "create" = FALSE
                   ))
  })
  
  # Dynamically render UI for Study ID selector
  output$studyIdUI <- renderUI({
    all_columns <- uploadedColnames() %||% character(0)
    id_columns <- grep("id$", all_columns, value = TRUE, ignore.case = TRUE)
    other_columns <- setdiff(all_columns, id_columns)
    choices <- if (length(all_columns) > 0) c(id_columns, " ", other_columns) else ""
    
    # Render the selectizeInput UI for the study ID with a visual separator
    selectizeInput("studyId", "Select Study ID", 
                   choices = choices, 
                   multiple = FALSE,
                   options = list(
                     "plugins" = list("remove_button"),
                     "maxItems" = 1,
                     "create" = FALSE
                   ))
  })
  
  # Dynamically render UI for Event Column selector
  output$eventColUI <- renderUI({
    all_columns <- uploadedColnames() %||% character(0)
    event_columns <- all_columns[all_columns %in% c("redcap_event_name", "Event Name")]
    other_columns <- setdiff(all_columns, event_columns)
    choices <- if (length(all_columns) > 0) c("None", " ", event_columns, " ", other_columns) else ""
    
    # Render the selectizeInput UI for the study ID with a visual separator
    selectizeInput("eventCol", "Select Event Column", 
                   choices = choices,
                   selected=event_columns[1],
                   multiple = FALSE,
                   options = list(
                     "plugins" = list("remove_button"),
                     "maxItems" = 1,
                     "create" = FALSE
                   ))
  })
  
  
  # Store the file metadata (no full dataset) when "Add Dataset" is clicked
  observeEvent(input$addDataset, {
    req(input$file, input$arestId, input$studyId, input$datasetName)
    
    rv$datasets[[length(rv$datasets) + 1]] <- list(
      path = input$file$datapath,      # Temporary file path
      filename = input$file$name,      # Original filename
      name = input$datasetName,        # User-provided name
      suffix = datasetSuffix(),
      key = input$arestId,             #  Arest ID column
      studyId = input$studyId,         # Study ID column
      eventCol = if (input$eventCol != 'None') input$eventCol else NA_character_,          # The redcap event column
      events = data.frame(             # Initialise an empty events data.frame
        original = character(),
        renamed = character(),
        dateColumn = character(),
        stringsAsFactors = FALSE
      )
    )
    
    # Store file names for reference
    rv$files <- c(rv$files, input$file$name)
    
    # Reset inputs after adding the dataset
    reset("file")
    updateTextInput(session, "datasetName", value = "")
    updateSelectInput(session, "arestId", selected = "")
    updateSelectInput(session, "studyId", selected = "")
    updateSelectInput(session, "eventCol", selected = "")
  })
  
  observe({
    is_ready <- !is.null(input$datasetName) && input$datasetName != "" &&
      !is.null(input$arestId) && input$arestId != "" && input$arestId != " " &&
      !is.null(input$studyId) && input$studyId != "" && input$studyId != " "
    
    if (is_ready) {
      shinyjs::enable("addDataset")
    } else {
      shinyjs::disable("addDataset")
    }
  })
  
  observe({  # monitor rv$datasets and enable/disable 'process datasets' button accordingly
    if (length(rv$datasets) > 0) {
      shinyjs::enable("processDatasets")
    } else {
      shinyjs::disable("processDatasets")
    }
  })
  
  # Render the table to display metadata for each uploaded dataset
  output$fileList <- renderDT({
    if (length(rv$datasets) == 0) return(data.frame())  # Empty table if no datasets
    
    # Create a data frame with metadata
    df <- data.frame(
      `Move` = paste0(
        '<button class="moveUp" id="up_', seq_along(seq_along(rv$datasets)), '">&#9650;</button>',
        '<button class="moveDown" id="down_', seq_along(seq_along(rv$datasets)), '">&#9660;</button>'),
      `Dataset Name` = sapply(rv$datasets, function(x) x$name),            # Dataset name
      `File name` = sapply(rv$datasets, function(x) x$filename),   # File name
      `Siffix` = sapply(rv$datasets, function(x) x$suffix),   # File name
      `Arest ID` = sapply(rv$datasets, function(x) x$key),        # Arest ID
      `Study ID` = sapply(rv$datasets, function(x) x$studyId),      # Study ID
      `Event Column` = sapply(rv$datasets, function(x) x$eventCol),      # Event Column
      `Delete` = paste0(
        '<button class="btn btn-danger btn-sm deleteBtn" id="delete_', seq_along(seq_along(rv$datasets)), '">Delete</button>'
      )
    )
    
    datatable(df,
              escape=FALSE,
              rownames=FALSE,
              selection='none',
              options=list(dom='t'),
              colnames = c("", "Dataset Name", "File Name", "Suffix", "Arest ID", "Study ID", "Events column", "Delete"),
              callback = JS(
                "table.on('click', 'button.moveUp', function() {",
                "  var id = this.id;",
                "  Shiny.setInputValue('move_up', id, {priority: 'event'});",
                "});",
                "table.on('click', 'button.moveDown', function() {",
                "  var id = this.id;",
                "  Shiny.setInputValue('move_down', id, {priority: 'event'});",
                "});",
                "table.on('click', 'button.deleteBtn', function() {",
                "  var id = this.id;",
                "  Shiny.setInputValue('delete_pressed', id, {priority: 'event'});",
                "});"
              )
            )
    
  }, server=FALSE)
  
  
  # Handle the move up button
  observeEvent(input$move_up, {
    index <- as.numeric(sub("up_", "", input$move_up))
    if (!is.na(index) && index > 1 && index <= length(rv$datasets)) {
      tmp <- rv$datasets[[index - 1]]
      rv$datasets[[index - 1]] <- rv$datasets[[index]]
      rv$datasets[[index]] <- tmp
    }
  })
  
  observeEvent(input$move_down, {
    index <- as.numeric(sub("down_", "", input$move_down))
    if (!is.na(index) && index < length(rv$datasets)) {
      tmp <- rv$datasets[[index + 1]]
      rv$datasets[[index + 1]] <- rv$datasets[[index]]
      rv$datasets[[index]] <- tmp
    }
  })
  
  # Handle the delete button
  observeEvent(input$delete_pressed, {
    req(input$delete_pressed)
    
    # Extract the index from the button ID (e.g., "delete_3" -> 3)
    index <- as.numeric(sub("delete_", "", input$delete_pressed))
    
    if (!is.na(index) && index <= length(rv$datasets)) {
      rv$datasets <- rv$datasets[-index]
    }
  })
  
  
  ############################
  ##### Process Datasets #####
  ############################
  
  observeEvent(input$processDatasets, {
    req(length(rv$datasets) > 0)
    
    progress_message("Processing datasets... Please wait.\n")
    
    for (i in seq_along(rv$datasets)) {
      ds <- rv$datasets[[i]]
      
      update_progress(paste("Processing dataset", ds$name, "[", i, " / ", length(rv$datasets), "]..."))
      data_processed <- process_dataset(ds$path, ds$key, ds$studyId, ds$name, ds$eventCol)
      
      # If processing succeeded, store the processed data in rv$datasets
      if (!is.null(data_processed)) {
        
        # Create events metadata frame
        event_names <- unique(data_processed$event_name)
        
        events <- tibble(
          original = event_names,
          renamed = if (nrow(ds$events) == 0) {
            str_to_title(gsub("_", " ", event_names))  # Attempt pretty event names
          } else {
            ds$events$renamed
          },
          dateColumn = if (nrow(ds$events) == 0) {
            NA_character_
          } else {
            ds$events$dateColumn
          }
        )
        
        rv$datasets[[which(sapply(rv$datasets, function(x) x$name) == ds$name)]]$data <- data_processed
        rv$datasets[[which(sapply(rv$datasets, function(x) x$name) == ds$name)]]$events <- events
      }
      
    }
    
    progress_message("All datasets have been processed.\n")
    
    # Enable the dropdown
    shinyjs::enable("selectedDataset")
    
    # jump to "Manage Events"
    updateTabsetPanel(session, "mainTabs", selected = "Manage Events")
    
  })
  
  # Render the progress message in UI
  output$progressText <- renderText({
    progress_message()  # Dynamically updated message
  })
  
  # UI to display progress
  output$progressUI <- renderUI({
    textOutput("progressText")
  })
  
  ##### Managing Events #####
  
  # Update dataset choices
  observe({
    current <- isolate(input$selectedDataset)
    choices <- sapply(rv$datasets, `[[`, "name")
    
    updateSelectInput(session, "selectedDataset",
                      choices = choices,
                      selected = if (current %in% choices) current else choices[1])
  })
  
  # Render UI for managing events (including unique events and column selection)
  output$eventManagerUI <- renderUI({
    req(input$selectedDataset)  # Ensure selected dataset exists
    
    ds <- rv$datasets[[which(sapply(rv$datasets, `[[`, "name") == input$selectedDataset)]]
    req(!is.null(ds$data), !is.null(ds$events))
    
    data <- ds$data
    events <- ds$events
    suffix <- ds$suffix
    
    # Define priority patterns and their order
    patterns <- c("date", "dt", "visit", "dob")
    
    # Define a function to assign priority
    date_priority <- function(name) {
      name_lower <- tolower(name)
      if (grepl("date", name_lower)) return(1)
      if (grepl("dt", name_lower)) return(2)
      if (grepl("visit", name_lower)) return(3)
      if (grepl("dob", name_lower)) return(4)
      return(5)  # lowest priority if none match (optional fallback)
    }
    
    event_rows <- purrr::map(seq_len(nrow(events)), function(i) {
      event_orig <- events$original[i]
      event_renamed <- events$renamed[i]
      selected_date <- events$dateColumn[i]
      
      data_event <- data %>% filter(event_name == event_orig)
      
      # Keep only columns with any non-NA values (excluding redcap_event_name)
      non_empty_cols <- data_event %>%
        select(where(~ any(!is.na(.))))
      
      data_filtered <- bind_cols(
        event_name = data_event$event_name,
        non_empty_cols
      )
      
      date_columns <- data_filtered %>%
        select(where(~ inherits(., "Date") || inherits(., "POSIXt")),
               matches(paste(patterns, collapse = '|'), ignore.case = TRUE)) %>%
        names()
      
      # Sort the date-like columns by priority
      ordered_cols <- date_columns[order(map_int(date_columns, date_priority))]
      
      choices <- if (length(ordered_cols) > 0) c("None", ordered_cols) else c("None", names(data_filtered))
      
      fluidRow(
        column(5, textInput(paste0("rename_event_", suffix, "_", i),
                            label = event_orig,
                            value = event_renamed)),
        column(3, selectInput(paste0("date_", suffix, "_", i),
                              label = " ",
                              choices = choices,
                              selected = if (!is.na(selected_date)) selected_date else if (length(ordered_cols) > 0) ordered_cols[1] else "None"))
      )
      
    })
    
    # Return the event manager UI with a table and the save button
    tagList(
      fluidRow(
        column(5, h4("Event Name")),  # Event Name column header
        column(3, h4("Select Date Column"))  # Date Column header
      ), br(),
      event_rows
    )
    
  })
  
  saveEventChanges <- function(ds) {
    req(!is.null(ds$events))
    
    updated_renames <- character(nrow(ds$events))
    updated_dates <- character(nrow(ds$events))
    suffix = ds$suffix
    
    for (i in seq_len(nrow(ds$events))) {
      rename_input <- input[[paste0("rename_event_", suffix, "_", i)]]
      date_input <- input[[paste0("date_", suffix, "_", i)]]
      
      updated_renames[i] <- if (!is.null(rename_input)) rename_input else ds$events$original[i]
      updated_dates[i] <- if (!is.null(date_input) && date_input != "None") date_input else NA
    }
    
    ds$events$renamed <- updated_renames
    ds$events$dateColumn <- updated_dates
    
    return(ds)
  }
  
  
  # Save changes back into rv
  observeEvent(input$saveEventChanges, {
    req(input$selectedDataset)
    
    dataset_index <- which(sapply(rv$datasets, `[[`, "name") == input$selectedDataset)
    updated_ds <- saveEventChanges(rv$datasets[[dataset_index]])
    
    rv$datasets[[dataset_index]] <- updated_ds
    
    print(rv$datasets[[dataset_index]]$events)
    showNotification("Event changes saved.", type = "message")
  })

  
  ##### Merge Datasets #####
  
  # Merge the datasets on the arest ID and study ID columns when button is clicked
  observeEvent(input$mergeDatasets, {
    req(length(rv$datasets) >= 1)
    
    # First just make sure to save any changes to the event mappings
    for (i in seq_along(rv$datasets)) {
      rv$datasets[[i]] <- saveEventChanges(rv$datasets[[i]])
    }
    
    ### Apply the renewed event names and date columns 
    for (i in seq_along(rv$datasets)) {
      dataset <- rv$datasets[[i]]
      df <- dataset$data
      events <- dataset$events

      df <- df %>%
        mutate(
          event_name = recode(event_name, !!!setNames(events$renamed, events$original)),
          
          event_date = map2(event_name, seq_len(n()), function(ev, row_idx) {
            date_col <- events$dateColumn[events$renamed == ev]
            
            if (!is.na(date_col) && date_col %in% names(df)) {
              date_value <- df[[date_col]][row_idx]
              
              # If already a Date or POSIXt object, coerce to Date
              if (inherits(date_value, "Date")) {
                date_value
              } else if (inherits(date_value, "POSIXt")) {
                as.Date(date_value)
              } else {
                # Try to parse the string into a date
                parsed_date <- tryCatch({
                  parse_date_time(date_value, 
                                  orders = c("ymd", "dmy", "mdy", "B d, Y", "d b Y", "d B Y")) %>%
                    as.Date()
                }, error = function(e) NA)
                parsed_date
              }
            } else {
              NA_Date_
            }
          }),
          
          event_date_str = map2_chr(event_name, seq_len(n()), function(ev, row_idx) {
            date_col <- events$dateColumn[events$renamed == ev]
            if (!is.na(date_col) && date_col %in% names(df)) {
              as.character(df[[date_col]][row_idx])  # Keep the original string
            } else {
              NA_character_
            }
          }),
          
          event_date_source = map_chr(event_name, ~ {
            col <- events$dateColumn[events$renamed == .x]
            if (length(col) > 0) col else NA_character_
          })
        )
      
      # Store updated dataframe and events back into rv
      rv$datasets[[i]]$data <- df
      
    }
    
    shared_cols <- c("arest_id", "source", "study_id", "event_name", "event_date", "event_date_str", "event_date_source", "redcap_repeat_instrument", "redcap_repeat_instance")
    overlap_cols_list = list()
    for (i in seq_along(rv$datasets)) {
      if (is.null(rv$datasets[[i]]$data)) next  # skip if not processed
      target_cols <- names(rv$datasets[[i]]$data)
      target_name <- rv$datasets[[i]]$name
      
      other_cols <- lapply(rv$datasets[-i], function(ds) names(ds$data))
      other_cols <- Filter(Negate(is.null), other_cols)
      
      overlap_cols <- do.call(
        get_overlapping_columns,
        c(list(target_cols = target_cols, shared_cols = shared_cols), other_cols)
      )
      
      overlap_cols_list[[target_name]] <- overlap_cols
      
    }
    
    # Now rename the overlapping columns
    for (target_name in names(overlap_cols_list)){
      overlap_cols <- overlap_cols_list[[target_name]]
      
      if (length(overlap_cols) == 0) next  # Skip if no overlapping columns
      
      # Get the dataset
      ds_index <- which(sapply(rv$datasets, function(x) x$name) == target_name)
      df <- rv$datasets[[ds_index]]$data
      suffix <- rv$datasets[[ds_index]]$suffix
      
      df <- df %>%
        rename_with(~ paste0(.x, "_", suffix), .cols = all_of(overlap_cols))
      
      # Update the dataset in rv$datasets
      rv$datasets[[ds_index]]$data <- df
      
    }
    
    rv$mergedData <- rv$datasets %>%
      map("data") %>%                     # Extract the $data from each dataset
      bind_rows() %>%                    # Combine them into one dataframe
      select(arest_id, source, study_id, event_name, event_date, event_date_str, event_date_source, everything()) %>%    # Reorder columns
      arrange(arest_id)
    
    shinyjs::enable("preview")
    
  })
  
  observeEvent(input$preview, {
    View(rv$mergedData)
  })
  
}



shinyApp(ui, server)