#' @title Defect assessment and summary
#'
#' @description
#' `defectAssessment` returns a tibble which contains the summary information on all defects
#'
#' @details
#' The `defectAssessment` function is a helper function to return a useful tibble to allow more detailed analysis.
#' The function returns all recorded defects by season and grader batch.  It also integrates the pack-out value to
#' deliver the defect proportion of the entire batch.
#'
#' @param seasons a vector of the seasons to be returned.
#' @param password a text string containing the password to the SQL database (see administrator for the password string)
#' @return a tibble with each row representing a specific defect for a specific grader batch.
#' @export
#' @seealso [graderFunction()], [binsHarvested()], [growerRTE()]

# Function to deliver defect data

defectAssessment <- function(seasons, password) {

  #selectedTemplates <- c("Grade Analysis", "Packer Rejects", "Defect Sorter Rejects", "Class 1.5 Analysis")
  #selectedTemplates2019 <- c("Reject Analysis", "Packer Rejects", "Defect Sorter Rejects")
  selectedTemplates <- c("Grade Analysis")
  selectedTemplates2019 <- c("Reject Analysis")

  if(max(seasons) >= 2020) {

    con <- DBI::dbConnect(odbc::odbc(),
                          Driver = "ODBC Driver 17 for SQL Server",
                          Server = "abcrepldb.database.windows.net",
                          Database = "ABCPackerRepl",
                          UID = "abcadmin",
                          PWD = password,
                          Port = 1433
    )

    defect_assessments <- dplyr::left_join(dplyr::tbl(con,"qa_Assessment_DefectT") |>
                                      dplyr::select(c(AssessmentDefectID, AssessmentID, DefectID, DefectQty)),
                                    dplyr::tbl(con, "qa_AssessmentT") |>
                                      dplyr::select(c(AssessmentID, GraderBatchID, BlockID, TemplateID, FarmID,
                                               GradeID, SampleQty, SeasonID)),
                                    by = "AssessmentID") |>
      dplyr::left_join(dplyr::tbl(con, "qa_TemplateT") |> dplyr::select(c(TemplateID, TemplateName)),
                by = "TemplateID") |>
      dplyr::left_join(dplyr::tbl(con, "qa_DefectT") |> dplyr::select(c(DefectID, Defect)),
                by = "DefectID") |>
      dplyr::mutate(Season = dplyr::case_when(SeasonID == 6 ~ 2020,
                                SeasonID == 7 ~ 2021,
                                SeasonID == 8 ~ 2022,
                                SeasonID == 9 ~ 2023)) |>
      dplyr::filter(TemplateName %in% {{selectedTemplates}},
             Season %in% {{seasons}}) |>
      dplyr::select(-c(TemplateID, SeasonID, GradeID, FarmID, BlockID, DefectID)) |>
      dplyr::collect()

    sampleQty <- dplyr::tbl(con, "qa_AssessmentT") |>
      dplyr::left_join(dplyr::tbl(con, "qa_TemplateT") |> dplyr::select(c(TemplateID, TemplateName)),
                by = "TemplateID") |>
      dplyr::mutate(Season = dplyr::case_when(SeasonID == 6 ~ 2020,
                                SeasonID == 7 ~ 2021,
                                SeasonID == 8 ~ 2022,
                                SeasonID == 9 ~ 2023)) |>
      dplyr::filter(!is.null(GraderBatchID),
             TemplateName %in% {{selectedTemplates}},
             Season %in% {{seasons}}) |>
      dplyr::group_by(Season, GraderBatchID) |>
      dplyr::summarise(sampleQty = sum(SampleQty, na.rm=T),
                .groups = "drop") |>
      dplyr::collect()

    DBI::dbDisconnect(con)

  } else {

    defect_assessments <- tidyr::tibble(
      AssessmentDefectID = integer(),
      AssessmentID = integer(),
      DefectQty = integer(),
      GraderBatchID = integer(),
      SampleQty = integer(),
      TemplateName = character(),
      Defect = character(),
      Season = numeric()
    )

    sampleQty <- tidyr::tibble(
      Season = numeric(),
      GraderBatchID = integer(),
      sampleQty = integer()
    )
  }

  if(min(seasons) < 2020) {

    con <- DBI::dbConnect(odbc::odbc(),
                          Driver = "ODBC Driver 17 for SQL Server",
                          Server = "abcrepldb.database.windows.net",
                          Database = "ABCPackRepl",
                          UID = "abcadmin",
                          PWD = password,
                          Port = 1433
    )


    defect_assessments2019 <- dplyr::left_join(dplyr::tbl(con,"qa_Assessment_DefectT") |>
                                          dplyr::select(c(AssessmentDefectID, AssessmentID, DefectID, DefectQty)),
                                        dplyr::tbl(con, "qa_AssessmentT") |>
                                          dplyr::select(c(AssessmentID, GraderBatchID, BlockID, TemplateID,
                                                   FarmID, GradeID, SampleQty, SeasonID)),
                                        by = "AssessmentID") |>
      dplyr::left_join(dplyr::tbl(con, "qa_TemplateT") |> dplyr::select(c(TemplateID, TemplateName)),
                by = "TemplateID") |>
      dplyr::left_join(dplyr::tbl(con, "qa_DefectT") |> dplyr::select(c(DefectID, Defect)),
                by = "DefectID") |>
      dplyr::mutate(Season = dplyr::case_when(SeasonID == 2 ~ 2016,
                                SeasonID == 3 ~ 2017,
                                SeasonID == 4 ~ 2018,
                                SeasonID == 5 ~ 2019)) |>
      dplyr::select(-c(TemplateID, SeasonID, GradeID, FarmID, BlockID, DefectID)) |>
      dplyr::filter(TemplateName %in% {{selectedTemplates2019}},
             Season %in% {{seasons}}) |>
      dplyr::collect()

    sampleQty2019 <- dplyr::tbl(con, "qa_AssessmentT") |>
      dplyr::left_join(dplyr::tbl(con, "qa_TemplateT") |> dplyr::select(c(TemplateID, TemplateName)),
                by = "TemplateID") |>
      dplyr::mutate(Season = dplyr::case_when(SeasonID == 2 ~ 2016,
                                SeasonID == 3 ~ 2017,
                                SeasonID == 4 ~ 2018,
                                SeasonID == 5 ~ 2019)) |>
      dplyr::filter(!is.null(GraderBatchID),
             TemplateName %in% {{selectedTemplates2019}},
             Season %in% {{seasons}}) |>
      dplyr::group_by(Season, GraderBatchID) |>
      dplyr::summarise(sampleQty = sum(SampleQty, na.rm=T),
                .groups = "drop") |>
      dplyr::collect()

    DBI::dbDisconnect(con)

  } else {

    defect_assessments2019 <- tidyr::tibble(
      AssessmentDefectID = integer(),
      AssessmentID = integer(),
      DefectQty = integer(),
      GraderBatchID = integer(),
      SampleQty = integer(),
      TemplateName = character(),
      Defect = character(),
      Season = numeric()
    )

    sampleQty2019 <- tidyr::tibble(
      Season = numeric(),
      GraderBatchID = integer(),
      sampleQty = integer()
    )

  }

  gbd <- graderFunction(seasons, password)

  sq <- sampleQty2019 |>
    dplyr::bind_rows(sampleQty)

  defAss <- defect_assessments2019 |>
    dplyr::bind_rows(defect_assessments)

  da <- defAss |>
    dplyr::group_by(Season, GraderBatchID, Defect) |>
    dplyr::summarise(defectQty = sum(DefectQty),
              .groups = "drop") |>
    dplyr::left_join(sq, by=c("Season", "GraderBatchID")) |>
    dplyr::left_join(gbd, by = c("Season", "GraderBatchID")) |>
    dplyr::mutate(proportion = (1-packOut)*defectQty/sampleQty,
           Defect = ifelse(Defect == "Russet Cheek" | Defect == "Russet Stem", "Russet", Defect)) |>
    dplyr::filter(!is.na(FarmName))

  return(da)

}
