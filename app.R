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

library(dplyr)
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


process_dataset <- function(path, arestid_col, studyid_col, dataset_name) {
  df0 <- read_data(path)
  
  if (!"redcap_event_name" %in% names(df0)) {
    df0$redcap_event_name <- dataset_name
  }
  
  df <- df0 %>%
    rename(arest_id = !!sym(arestid_col),
           study_id = !!sym(studyid_col)) %>%
    fill(arestid) %>%
    mutate(source = dataset_name) %>%
    select(arest_id, study_id, everything())
  
  return(df_processed)
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
  
  sidebarLayout(
    sidebarPanel(
      h4("Select Datasets"),
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
      
      # Dynamically render column selectors for arest id and study ID
      div(id = "arestIdWrapper", uiOutput("arestIdUI")),
      div(id = "studyIdWrapper", uiOutput("studyIdUI")),
      
      # Button to add dataset
      actionButton("addDataset", "Add Dataset", class = "btn-primary", disabled = TRUE)
    ),
    
    mainPanel(
      h4("Current Datasets"),
      
      # Table displaying the list of datasets with their metadata
      DTOutput("fileList"),
      
      # Displaying the preview table
      DTOutput("previewTable"),
      
      # Buttons container (bottom-right)
      tags$div(
        id = "actionButtonsContainer",
        actionButton("mergeDatasets", "Merge Datasets"),
        actionButton("preview", "Preview Merged Data")
      )
    )
  )
)

server <- function(input, output, session) {
  
  # Reactive value to store datasets and metadata
  rv <- reactiveValues(datasets = list(), files = character())
  
  # Reactively get column names (no data loaded yet)
  uploadedColnames <- reactiveVal(NULL)
  
  observeEvent(input$file, {
    req(input$file)
    
    tryCatch({
      cols <- read_headers(input$file$datapath)
      uploadedColnames(cols)
    }, error = function(e) {
      showNotification(paste("Error reading file:", e$message), type = "error")
      uploadedColnames(NULL)
    })
  })
  
  observe({
    if (is.null(uploadedColnames())) {
      shinyjs::addClass("arestIdWrapper", "disabled-block")
      shinyjs::addClass("studyIdWrapper", "disabled-block")
    } else {
      shinyjs::removeClass("arestIdWrapper", "disabled-block")
      shinyjs::removeClass("studyIdWrapper", "disabled-block")
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
  
  # Dynamically render UI for Study ID selector (similar to arest ID)
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
  

  
  # Store the file metadata (no full dataset) when "Add Dataset" is clicked
  observeEvent(input$addDataset, {
    req(input$file, input$arestId, input$studyId, input$datasetName)
    
    # Store only metadata (file path, name, arest ID, study ID)
    rv$datasets[[length(rv$datasets) + 1]] <- list(
      path = input$file$datapath,      # Temporary file path
      filename = input$file$name,      # Original filename
      name = input$datasetName,        # User-provided name
      key = input$arestId,          # Arest ID column
      studyId = input$studyId        # Study ID column
    )
    
    # Store file names for reference
    rv$files <- c(rv$files, input$file$name)
    
    # Reset inputs after adding the dataset
    reset("file")
    updateTextInput(session, "datasetName", value = "")
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
      `Arest ID` = sapply(rv$datasets, function(x) x$key),        # Arest ID
      `Study ID` = sapply(rv$datasets, function(x) x$studyId),      # Study ID
      `Delete` = paste0(
        '<button class="btn btn-danger btn-sm deleteBtn" id="delete_', seq_along(seq_along(rv$datasets)), '">Delete</button>'
      )
    )
    
    datatable(df,
              escape=FALSE,
              rownames=FALSE,
              selection='none',
              options=list(dom='t'),
              colnames = c("", "Dataset Name", "File Name", "Arest ID", "Study ID", "Delete"),
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
  
  # Merge the datasets on the arest ID and study ID columns when button is clicked
  mergedData <- eventReactive(input$mergeDatasets, {
    req(length(rv$datasets) > 1)
    
    # Read the first dataset
    first <- read_csv(rv$datasets[[1]]$path)
    merged <- first
    
    # Merge datasets one by one
    for (i in 2:length(rv$datasets)) {
      nextData <- read_csv(rv$datasets[[i]]$path)
      merged <- full_join(merged, nextData, by = c(rv$datasets[[i]]$key, rv$datasets[[i]]$studyId))
    }
    
    merged
  })
  
  # Show preview of merged data when button is clicked
  output$previewTable <- renderDT({
    req(input$mergeDatasets)
    datatable(mergedData())
  })
}



shinyApp(ui, server)