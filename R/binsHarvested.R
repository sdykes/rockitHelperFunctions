#' @title bin harvested summary
#'
#' @description
#' `binsHarvested` returns a tibble which contains the summary information on bins harvested
#'
#' @details
#' The `binsHarvested` function is a helper function to return a useful tibble to allow more detailed analysis
#' of bins submitted into the ABC system (at the gatehouse). The function returns all recorded bin consignments by season
#' and Bin delivery number.
#'
#' @param seasons a vector of the seasons to be returned.
#' @param password a text string containing the password to the SQL database (see administrator for the password string)
#' @return a tibble with each row representing a specific bin consignment.
#' @export
#' @seealso [graderFunction()], [defectAssessment()], [growerRTE()]

# function to determine bins harvested

binsHarvested <- function(seasons, password) {

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver = "ODBC Driver 18 for SQL Server",
                        Server = "abcrepldb.database.windows.net",
                        Database = "ABCPacker2023Repl",
                        UID = "abcadmin",
                        PWD = password,
                        Port = 1433
  )

  binsCompany2020 <- dplyr::tbl(con, "ma_Bin_DeliveryT") |>
    dplyr::filter(PresizeFlag == 0) |>
    dplyr::select(BinDeliveryID, SeasonID,
                  FirstStorageSiteCompanyID) |>
    dplyr::left_join(dplyr::tbl(con, "sw_CompanyT") |> dplyr::select(c(CompanyID, CompanyName)),
              by = c("FirstStorageSiteCompanyID" = "CompanyID")) |>
    dplyr::mutate(StorageSite = case_when(CompanyName == "Te Ipu Packhouse (RO)" & SeasonID > 6 ~ "Te Ipu",
                                   CompanyName == "Te Ipu Packhouse (RO)" & SeasonID <= 6 ~ "Cooper Street",
                                   CompanyName == "Berl Property Ltd" ~ "Raupare Rd",
                                   TRUE ~ "Raupare Rd")) |>
    dplyr::select(-c(SeasonID, CompanyName, FirstStorageSiteCompanyID)) |>
    dplyr::collect()

  binsHarvested2020 <- dplyr::tbl(con, "ma_Bin_DeliveryT") |>
    dplyr::select(BinDeliveryID,
                  SeasonID,
                  BinDeliveryNo,
                  FarmID,
                  BlockID,
                  HarvestDate,
                  NoOfBins,
                  MaturityID,
                  StorageTypeID,
                  PresizeFlag,
                  TagID,
                  TreatmentID,
                  Comment,
                  ReceivedDate,
                  PickNoID,
                  GrowingTypeID) |>
    dplyr::left_join(dplyr::tbl(con, "sw_Farm_BlockT") |> dplyr::select(BlockID, BlockCode, BlockName, SubdivisionID),
              by="BlockID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_FarmT") |> dplyr::select(c(FarmID, FarmCode, FarmName, GrowerCompanyID)),
              by = "FarmID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_SubdivisionT") |> dplyr::select(c(SubdivisionID, SubdivisionCode,
                                                                         SubdivisionDesc)),
                     by = "SubdivisionID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_CompanyT") |> dplyr::select(c(CompanyID, CompanyName)),
              by = c("GrowerCompanyID" = "CompanyID")) |>
    dplyr::rename(owner = CompanyName) |>
    dplyr::left_join(dplyr::tbl(con, "sw_Pick_NoT") |> dplyr::select(c(PickNoID, PickNoDesc)),
              by = "PickNoID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_TreatmentT") |> dplyr::select(c(TreatmentID, TreatmentDesc)),
              by = "TreatmentID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_TagT") |> dplyr::select(c(TagID, TagDesc)),
              by = "TagID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Growing_TypeT") |> dplyr::select(c(GrowingTypeID, GrowingTypeDesc)),
              by = "GrowingTypeID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_MaturityT") |> dplyr::select(c(MaturityID, MaturityCode)),
              by = "MaturityID") |>
    dplyr::mutate(Season = dplyr::case_when(SeasonID == 6 ~ 2020,
                              SeasonID == 7 ~ 2021,
                              SeasonID == 8 ~ 2022,
                              SeasonID == 9 ~ 2023),
           StorageType = dplyr::case_when(StorageTypeID == 4 ~ "CA",
                                   StorageTypeID == 7 ~ "RA",
                                   TRUE ~ "RA")) |>
    dplyr::filter(PresizeFlag == 0) |>
    dplyr::rename(ConsignmentNumber = BinDeliveryNo) |>
    dplyr::select(-c(SeasonID,
              PresizeFlag,
              FarmID,
              BlockID,
              SubdivisionID,
              StorageTypeID,
              PickNoID,
              TreatmentID,
              TagID,
              GrowingTypeID,
              GrowerCompanyID,
              MaturityID)) |>
    dplyr::relocate(Season, .after = ConsignmentNumber) |>
    dplyr::collect() |>
    dplyr::mutate(owner = stringr::str_trim(owner, side = "both"),
           owner = stringr::str_to_lower(owner),
           owner = stringr::str_replace_all(owner, " ", ""))

  DBI::dbDisconnect(con)

  bh2020 <- binsHarvested2020 |>
    dplyr::left_join(binsCompany2020, by = "BinDeliveryID")

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver = "ODBC Driver 18 for SQL Server",
                        Server = "abcrepldb.database.windows.net",
                        Database = "ABCPackRepl",
                        UID = "abcadmin",
                        PWD = password,
                        Port = 1433
  )

  binsCompany2019 <- dplyr::tbl(con, "ma_BinT") |>
    dplyr::filter(PresizeFlag == 0) |>
    dplyr::select(c(BinID, FirstStorageSiteCompanyID))|>
    dplyr::left_join(dplyr::tbl(con, "sw_CompanyT") |> dplyr::select(c(CompanyID, CompanyName)),
              by = c("FirstStorageSiteCompanyID" = "CompanyID")) |>
    dplyr::mutate(StorageSite = dplyr::case_when(CompanyName == "Rockit Packing Company Ltd" ~ "Cooper Street",
                                   CompanyName == "Berl Property Ltd" ~ "Raupare Rd",
                                   CompanyName == "Bostock New Zealand" ~ "Henderson Road",
                                   CompanyName == "T & G (Apollo)" ~ "T&G Whakatu",
                                   CompanyName == "Everest Kool SolutioNZ" ~ "Everest Kool Omahu Rd",
                                   CompanyName == "Crasborn Fresh Harvest " ~ "FreshMax Omahu Road",
                                   CompanyName == "2019 Crasborn Fresh Harvest" ~ "Raupare Rd",
                                   TRUE ~ CompanyName)) |>
    dplyr::select(-c(FirstStorageSiteCompanyID, CompanyName)) |>
    dplyr::rename(BinDeliveryID = BinID) |>
    dplyr::collect()

  binsHarvested2019 <- dplyr::tbl(con, "ma_BinT") |>
    dplyr::filter(PresizeFlag == 0) |>
    dplyr::select(c(BinID, BinNo, SeasonID, HarvestDate, NoOfBins, FarmID, BlockID,
             StorageTypeID, PresizeFlag, FirstStorageSiteCompanyID, Comment,
             ESPID, BinReceivedDate))|>
    dplyr::left_join(dplyr::tbl(con, "sw_FarmT") |> dplyr::select(FarmID, FarmCode, FarmName, GrowerCompanyID),
                     by = "FarmID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Farm_BlockT") |> dplyr::select(c(BlockID, BlockCode, BlockName, SubdivisionID)),
                     by = "BlockID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_SubdivisionT") |> dplyr::select(c(SubdivisionID, SubdivisionCode, SubdivisionDesc)),
                     by = "SubdivisionID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_CompanyT") |> dplyr::select(c(CompanyID, CompanyName)),
                     by = c("GrowerCompanyID" = "CompanyID")) |>
    dplyr::rename(owner = CompanyName) |>
    dplyr::left_join(dplyr::tbl(con, "sw_ESPT") |> dplyr::select(c(ESPID, ESPCode)),
                     by = "ESPID") |>
    dplyr::mutate(Season = dplyr::case_when(SeasonID == 2 ~ 2016,
                              SeasonID == 3 ~ 2017,
                              SeasonID == 4 ~ 2018,
                              SeasonID == 5 ~ 2019),
           StorageType = dplyr::case_when(StorageTypeID == 4 ~ "CA",
                                   StorageTypeID == 7 ~ "RA",
                                   TRUE ~ "RA")) |>
    dplyr::select(-c(FarmID, BlockID, StorageTypeID, SeasonID, ESPID,
                     PresizeFlag, GrowerCompanyID)) |>
    dplyr::rename(BinDeliveryID = BinID,
           ConsignmentNumber = BinNo,
           ReceivedDate = BinReceivedDate,
           MaturityCode = ESPCode) |>
    dplyr::mutate(ReceivedTime = as.character(NA),
           PickNoDesc = as.character(NA),
           TreatmentDesc = as.character(NA),
           TagDesc = as.character(NA),
           GrowingTypeDesc = as.character(NA)) |>
    dplyr::select(c(BinDeliveryID,
                    ConsignmentNumber,
                    Season,
                    HarvestDate,
                    NoOfBins,
                    Comment,
                    ReceivedDate,
                    BlockCode,
                    BlockName,
                    FarmCode,
                    FarmName,
                    SubdivisionCode,
                    SubdivisionDesc,
                    owner,
                    PickNoDesc,
                    TreatmentDesc,
                    TagDesc,
                    GrowingTypeDesc,
                    MaturityCode,
                    StorageType)) |>
    dplyr::filter(Season == lubridate::year(HarvestDate)) |>
    dplyr::collect() |>
    dplyr::mutate(owner = stringr::str_trim(owner, side = "both"),
           owner = stringr::str_to_lower(owner),
           owner = stringr::str_replace_all(owner, " ", ""))

  DBI::dbDisconnect(con)

  bh2019 <- binsHarvested2019 |>
    dplyr::left_join(binsCompany2019, by = "BinDeliveryID")

  binsHarvested <- bh2019 |>
    dplyr::bind_rows(bh2020) |>
    dplyr::filter(Season %in% {{seasons}})

  return(binsHarvested)
}
