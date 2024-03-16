#' @title Grader batch summary function
#'
#' @description
#' `graderFunction` returns a tibble which contains the summary information on all batches processed through the packhouse
#'
#' @details
#' The `graderFunction` function is a helper function to return a useful tibble to allow more detailed analysis
#' of bins processed through the packhouse and ABC system. The function returns all recorded batches by season
#' and grader batch number.
#'
#' @param seasons a vector of the seasons to be returned.
#' @param password a text string containing the password to the SQL database (see administrator for the password string)
#' @return a tibble with each row representing a specific bin consignment.
#' @export
#' @seealso [binsHarvested()], [defectAssessment()], [growerRTE()]

# Function to determine grader batch data

graderFunction <- function(seasons, password) {

  if(max(seasons) >= 2020) {

    con <- DBI::dbConnect(odbc::odbc(),
                          Driver = "ODBC Driver 18 for SQL Server",
                          Server = "abcrepldb.database.windows.net",
                          Database = "ABCPacker2023Repl",
                          UID = "abcadmin",
                          PWD = password,
                          Port = 1433
    )
    #
    # the juice fruit is now being put through the Bin_Delivery table with PresizeProdutID = 278
    #
    juiceFruitKgs <- dplyr::tbl(con, "ma_Bin_DeliveryT") |>
      dplyr::select(-ReceivedTime) |> # for some reason the R SQL conversion dosn't like this field I am not sure why
      dplyr::mutate(Season = dplyr::case_when(SeasonID == 6 ~ 2020,
                                SeasonID == 7 ~ 2021,
                                SeasonID == 8 ~ 2022,
                                SeasonID == 9 ~ 2023)) |>
      dplyr::filter(PresizeProductID == 278,
             Season %in% {{seasons}}) |>
      dplyr::group_by(PresizeOutputFromGraderBatchID) |> #this is the GraderBatchID
      dplyr::summarise(juiceKgs = sum(TotalWeight, na.rm=T),
                .groups = "drop") |>
      dplyr::rename(GraderBatchID = PresizeOutputFromGraderBatchID) |>
      dplyr::collect()
    #
    # Sample fruit is the undersize and oversize fruit and the sample fruit also.
    #
    sampleFruitKgs <- dplyr::left_join(dplyr::tbl(con, "ma_Pallet_DetailT"),
                                dplyr::tbl(con, "sw_ProductT") |> dplyr::select(c(ProductID, NetFruitWeight, SampleFlag)), by = "ProductID") |>
      dplyr::mutate(Season = dplyr::case_when(CreatedDateTime >= as.POSIXct("2020-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2021-01-01 00:00:00.000") ~ 2020,
                                CreatedDateTime >= as.POSIXct("2021-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2022-01-01 00:00:00.000") ~ 2021,
                                CreatedDateTime >= as.POSIXct("2022-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2023-01-01 00:00:00.000") ~ 2022,
                                CreatedDateTime >= as.POSIXct("2023-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2024-01-01 00:00:00.000") ~ 2023)) |>
      dplyr::filter(Season %in% {{seasons}}) |>
      dplyr::filter(SampleFlag == 1) |>
      dplyr::group_by(GraderBatchID) |>
      dplyr::summarise(sampleKgs = sum(NoOfUnits*NetFruitWeight, na.rm=T)) |>
      dplyr::collect()
    #
    # WasteOtherKgs makes up the rejectKgs.  Also the compac reconciliation
    # hence the negative numbers in some cases
    #
    GraderBatchT <- dplyr::left_join(dplyr::tbl(con, "ma_Grader_BatchT"),
                              dplyr::tbl(con, "sw_FarmT") |> dplyr::select(c(FarmID, FarmCode, FarmName)),
                              by = "FarmID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_Farm_BlockT") |> dplyr::select(c(BlockID, BlockCode, BlockName,SubdivisionID)), by ="BlockID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_MaturityT") |> dplyr::select(c(MaturityID, MaturityCode)), by = "MaturityID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_CompanyT") |> dplyr::select(c(CompanyID, CompanyName)), by = c("GrowerCompanyID" = "CompanyID")) |>
      dplyr::left_join(dplyr::tbl(con, "ma_ShiftT") |> dplyr::select(ShiftID, ShiftCode), by = "ShiftID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_SubdivisionT") |> dplyr::select(c(SubdivisionID, SubdivisionCode, SubdivisionDesc)),
                       by = "SubdivisionID") |>
      dplyr::rename(owner = CompanyName) |>
      dplyr::mutate(Season = dplyr::case_when(SeasonID == 6 ~ 2020,
                                SeasonID == 7 ~ 2021,
                                SeasonID == 8 ~ 2022,
                                SeasonID == 9 ~ 2023)) |>
      dplyr::filter(PresizeInputFlag == 0,
             Season %in% {{seasons}}) |>
      dplyr::select(c(GraderBatchID,
                      GraderBatchNo,
                      Season,
                      PackDate,
                      InputKgs,
                      ShiftCode,
                      GraderLineID,
                      WasteOtherKgs,
                      PresizeInputFlag,
                      StorageTypeID,
                      HarvestDate,
                      PickNoID,
                      FarmCode,
                      FarmName,
                      SubdivisionCode,
                      SubdivisionDesc,
                      BlockCode,
                      BlockName,
                      owner,
                      MaturityCode)) |>
      dplyr::collect()
    #
    # looseKgs calculation
    #
    looseFruitKgs1 <- dplyr::inner_join(dplyr::tbl(con, "ma_Export_Bin_DetailT"),
                                 dplyr::tbl(con, "ma_Pallet_DetailT") |> dplyr::select(c(PalletDetailID, GraderBatchID)),
                                 by = "PalletDetailID") |>
      dplyr::mutate(Season = dplyr::case_when(CreatedDateTime >= as.POSIXct("2020-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2021-01-01 00:00:00.000") ~ 2020,
                                CreatedDateTime >= as.POSIXct("2021-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2022-01-01 00:00:00.000") ~ 2021,
                                CreatedDateTime >= as.POSIXct("2022-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2023-01-01 00:00:00.000") ~ 2022,
                                CreatedDateTime >= as.POSIXct("2023-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2024-01-01 00:00:00.000") ~ 2023)) |>
      dplyr::filter(Season %in% {{seasons}}) |>
      dplyr::group_by(GraderBatchID) |>
      dplyr::summarise(looseKgs = sum(KGWeight, na.rm=T)) |>
      dplyr::collect()

    looseFruitKgs <- dplyr::tbl(con, "ma_Pallet_DetailT") |>
      dplyr::select(c(PalletDetailID, PalletID, ProductID, NoOfUnits, GrowerCompanyID, GraderBatchID, CreatedDateTime)) |>
      dplyr::left_join(dplyr::tbl(con, "sw_ProductT") |> dplyr::select(c(ProductID, GradeID, TubesPerCarton, NetFruitWeight)),
                by="ProductID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_GradeT") |> dplyr::select(c(GradeID, GradeDesc, PoolTypeID)),
                by="GradeID") |>
      dplyr::left_join(dplyr::tbl(con, "sys_fi_Pool_TypeT") |> dplyr::select(c(PoolTypeID, PoolByRule)),
                by = "PoolTypeID") |>
      dplyr::left_join(dplyr::tbl(con, "ma_Export_Bin_DetailT") |> dplyr::select(c(ExportBinDetailID, PalletDetailID, KGWeight)),
                by = "PalletDetailID") |>
      dplyr::mutate(Season = dplyr::case_when(CreatedDateTime >= as.POSIXct("2020-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2021-01-01 00:00:00.000") ~ 2020,
                                CreatedDateTime >= as.POSIXct("2021-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2022-01-01 00:00:00.000") ~ 2021,
                                CreatedDateTime >= as.POSIXct("2022-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2023-01-01 00:00:00.000") ~ 2022,
                                CreatedDateTime >= as.POSIXct("2023-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2024-01-01 00:00:00.000") ~ 2023)) |>
      dplyr::filter(Season %in% {{seasons}}) |>
      dplyr::collect() |>
      dplyr::filter(PoolByRule == "KG",
             is.na(ExportBinDetailID),
             GradeDesc != "Push Pack") |>
      dplyr::mutate(looseKgs = NoOfUnits*NetFruitWeight) |>
      dplyr::select(c(GraderBatchID, looseKgs)) |>
      dplyr::bind_rows(looseFruitKgs1) |>
      dplyr::group_by(GraderBatchID) |>
      dplyr::summarise(looseKgs = sum(looseKgs, na.rm=T),
                .groups = "drop")

    bin_qty <- dplyr::tbl(con, "ma_Bin_UsageT") |>
      dplyr::mutate(Season = dplyr::case_when(CreatedDateTime >= as.POSIXct("2020-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2021-01-01 00:00:00.000") ~ 2020,
                                CreatedDateTime >= as.POSIXct("2021-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2022-01-01 00:00:00.000") ~ 2021,
                                CreatedDateTime >= as.POSIXct("2022-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2023-01-01 00:00:00.000") ~ 2022,
                                CreatedDateTime >= as.POSIXct("2023-01-01 00:00:00.000") &
                                  CreatedDateTime < as.POSIXct("2024-01-01 00:00:00.000") ~ 2023)) |>
      dplyr::filter(Season %in% {{seasons}}) |>
      dplyr::collect() |>
      dplyr::select(GraderBatchID, BinQty) |>
      dplyr::group_by(GraderBatchID) |>
      dplyr::summarise(fieldBinsTipped = sum(BinQty, na.rm=T))

    DBI::dbDisconnect(con)

    graderBatchData2020 <- dplyr::left_join(GraderBatchT, juiceFruitKgs,
                                     by = "GraderBatchID") |>
      dplyr::left_join(sampleFruitKgs, by="GraderBatchID") |>
      dplyr::left_join(looseFruitKgs, by = "GraderBatchID") |>
      dplyr::mutate(dplyr::across(.cols = c(juiceKgs, sampleKgs, looseKgs), ~tidyr::replace_na(.x,0))) |>
      dplyr::left_join(bin_qty, by = "GraderBatchID") |>
      dplyr::filter(!is.na(WasteOtherKgs)) |>
      dplyr::mutate(InputKgs = InputKgs,
             rejectKgs = WasteOtherKgs + juiceKgs + sampleKgs + looseKgs,
             GraderBatchNo = as.integer(GraderBatchNo),
             packOut = 1-rejectKgs/InputKgs,
             storageDays = as.numeric(PackDate - HarvestDate),
             StorageType = dplyr::case_when(
               StorageTypeID == 4 ~ "CA",
               StorageTypeID == 7 ~ "RA",
               TRUE ~ "RA")) |>
      dplyr::mutate(owner = stringr::str_trim(owner, side = "both"),
             owner = stringr::str_to_lower(owner),
             owner = stringr::str_replace_all(owner, " ", "")) |>
      dplyr::mutate(ifelse(storageDays < 50 & StorageType == "CA", "RA", "RA")) |>
      dplyr::select(GraderBatchID,
             GraderBatchNo,
             Season,
             owner,
             FarmCode,
             FarmName,
             SubdivisionCode,
             SubdivisionDesc,
             BlockCode,
             BlockName,
             HarvestDate,
             PackDate,
             ShiftCode,
             storageDays,
             fieldBinsTipped,
             InputKgs,
             rejectKgs,
             packOut,
             MaturityCode,
             StorageType)
  } else {
#
# if the season don't include anything >= 2020 then return an empyty data frame
#
    graderBatchData2020 <- tibble::tibble(
      GraderBatchID = integer(),
      GraderBatchNo = integer(),
      Season = numeric(),
      owner = character(),
      FarmCode = character(),
      FarmName = character(),
      SubdivisionCode = character(),
      SubdivisionDesc = character(),
      BlockCode = character(),
      BlockName = character(),
      HarvestDate = date(),
      PackDate = date(),
      ShiftCode = character(),
      storageDays = numeric(),
      fieldBinsTipped = numeric(),
      InputKgs = numeric(),
      rejectKgs = numeric(),
      packOut = numeric(),
      MaturityCode = character(),
      StorageType = character()
    )
  }
  #
  #  code some logic here to test whether the older database needs to be queried
  #
  if(min(seasons) < 2020) {
    con <- DBI::dbConnect(odbc::odbc(),
                          Driver = "ODBC Driver 18 for SQL Server",
                          Server = "abcrepldb.database.windows.net",
                          Database = "ABCPackRepl",
                          UID = "abcadmin",
                          PWD = password,
                          Port = 1433
    )

    GraderBatchT2019 <- dplyr::tbl(con, "ma_Grader_BatchT") |>
      dplyr::left_join(dplyr::tbl(con, "sw_FarmT") |> dplyr::select(c(FarmID, FarmCode, FarmName, GrowerCompanyID)),
                                  by = "FarmID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_CompanyT") |> dplyr::select(CompanyID, CompanyName),
                by = c("GrowerCompanyID" = "CompanyID")) |>
      dplyr::rename(owner = CompanyName) |>
      dplyr::mutate(Season = dplyr::case_when(SeasonID == 2 ~ 2016,
                                SeasonID == 3 ~ 2017,
                                SeasonID == 4 ~ 2018,
                                SeasonID == 5 ~ 2019)) |>
      dplyr::filter(Season %in% {{seasons}}) |>
      dplyr::mutate(ShiftCode = "Day") |>
      dplyr::select(c(GraderBatchID,
               GraderBatchNo,
               Season,
               PackDate,
               InputKgs,
               ShiftCode,
               RejectKgs,
               owner,
               FarmCode,
               FarmName)) |>
      dplyr::collect()

    Bins2019 <- dplyr::tbl(con, "ma_Bin_UsageT") |>
      dplyr::group_by(GraderBatchID) |>
      dplyr::summarise(BinID = max(BinID, na.rm=T),
                BinQty = sum(BinQty, na.rm=T),
                .groups = "drop") |>
      dplyr::left_join(dplyr::tbl(con, "ma_BinT") |>
                  dplyr::select(c(BinID, SeasonID, FarmID, BlockID, NoOfBins, HarvestDate,
                           StorageTypeID, PresizeFlag, FirstStorageSiteCompanyID, ESPID)),
                by = "BinID")|>
      dplyr::left_join(dplyr::tbl(con, "sw_Farm_BlockT") |> dplyr::select(c(BlockID, BlockCode, BlockName, SubdivisionID)),
                       by = "BlockID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_SubdivisionT") |> dplyr::select(c(SubdivisionID, SubdivisionCode, SubdivisionDesc)),
                       by = "SubdivisionID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_Storage_TypeT") |> dplyr::select(c(StorageTypeID, StorageTypeDesc)),
                by = "StorageTypeID") |>
      dplyr::left_join(dplyr::tbl(con, "sw_CompanyT") |> dplyr::select(c(CompanyID, CompanyName)),
                by = c("FirstStorageSiteCompanyID" = "CompanyID")) |>
      dplyr::left_join(dplyr::tbl(con, "sw_ESPT") |> dplyr::select(c(ESPID, ESPCode)),
                by = "ESPID") |>
      dplyr::mutate(StorageSite = dplyr::case_when(CompanyName == "Rockit Packing Company Ltd" ~ "Cooper Street",
                                     CompanyName == "Berl Property Ltd" ~ "Raupare Rd",
                                     CompanyName == "Bostock New Zealand" ~ "Henderson Road",
                                     CompanyName == "T & G (Apollo)" ~ "T&G Whakatu",
                                     CompanyName == "Everest Kool SolutioNZ" ~ "Everest Kool Omahu Rd",
                                     CompanyName == "Crasborn Fresh Harvest " ~ "FreshMax Omahu Road",
                                     CompanyName == "2019 Crasborn Fresh Harvest" ~ "Raupare Rd",
                                     TRUE ~ CompanyName)) |>
      dplyr::select(-c(FarmID, BlockID, SubdivisionID, StorageTypeID,
                       FirstStorageSiteCompanyID, ESPID, CompanyName )) |>
      dplyr::collect()

    graderBatchData2019 <- GraderBatchT2019 |>
      dplyr::left_join(Bins2019 |> dplyr::filter(!is.na(GraderBatchID)) |>
                  dplyr::select(-c(BinID)),
                by = "GraderBatchID") |>
      dplyr::rename(rejectKgs = RejectKgs) |>
      dplyr::mutate(StorageType = dplyr::case_when(StorageTypeDesc == "Normal Air" ~ "RA",
                                     StorageTypeDesc == "CA & Smartfreshed" ~ "CA",
                                     TRUE ~ "RA"),
             packOut = 1-rejectKgs/InputKgs,
             storageDays = as.numeric(PackDate - HarvestDate),
             GraderBatchNo = as.integer(GraderBatchNo)) |>
      dplyr::mutate(owner = stringr::str_trim(owner, side = "both"),
             owner = stringr::str_to_lower(owner),
             owner = stringr::str_replace_all(owner, " ", "")) |>
      dplyr::select(-c(SeasonID,StorageTypeDesc, NoOfBins)) |>
      dplyr::rename(MaturityCode = ESPCode,
             fieldBinsTipped = BinQty)

    DBI::dbDisconnect(con)
#
# if the season don't include anything < 2020 then return an empty data frame
#
  } else {

    graderBatchData2019 <- tibble::tibble(
      GraderBatchID = integer(),
      GraderBatchNo = integer(),
      Season = numeric(),
      owner = character(),
      FarmCode = character(),
      FarmName = character(),
      SubdivisionCode = character(),
      SubdivisionDesc = character(),
      BlockCode = character(),
      BlockName = character(),
      HarvestDate = date(),
      PackDate = date(),
      ShiftCode = character(),
      storageDays = numeric(),
      fieldBinsTipped = numeric(),
      InputKgs = numeric(),
      rejectKgs = numeric(),
      packOut = numeric(),
      MaturityCode = character(),
      StorageType = character()
    )

  }

  gbd <- graderBatchData2019 |>
    dplyr::mutate(HarvestDate = as.Date(HarvestDate),
           PackDate = as.Date(PackDate)) |>
    dplyr::select(c(GraderBatchID,
             GraderBatchNo,
             Season,
             owner,
             FarmCode,
             FarmName,
             BlockCode,
             BlockName,
             SubdivisionCode,
             SubdivisionDesc,
             HarvestDate,
             PackDate,
             ShiftCode,
             storageDays,
             fieldBinsTipped,
             InputKgs,
             rejectKgs,
             packOut,
             MaturityCode,
             StorageType)) |>
    dplyr::bind_rows(graderBatchData2020)

  return(gbd)
}













