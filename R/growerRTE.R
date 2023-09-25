#' @title Grower RTE summary function
#'
#' @description
#' `growerRTEs` returns a tibble which contains the summary information on grower RTEs including the growerRTE category
#'
#' @details
#' The `growerRTE` function is a helper function to return a useful tibble to allow more detailed analysis
#' of growerRTEs for the various pack categories (i.e. tubes packed, re-packs, export bins and pre-sized bins). The function returns all grower RTEs by GraderBatchID and orchard and block information.
#'
#' @param seasons a vector of the seasons to be returned.
#' @param password the password string to the SQL database (see administrator for the password string)
#' @return a tibble with each row representing a specific grader batch.
#' @export
#' @seealso [graderFunction()], [defectAssessment()], [binsHarvested()]

# function to determine grower RTEs

growerRTE <- function(seasons, password) {

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver = "ODBC Driver 17 for SQL Server",
                        Server = "abcrepldb.database.windows.net",
                        Database = "ABCPackerRepl",
                        UID = "abcadmin",
                        PWD = password,
                        Port = 1433
  )

  tubeRTEInput <- dplyr::tbl(con, "ma_Pallet_DetailT") |>
    dplyr::select(c(PalletDetailID, ProductID, NoOfUnits, GraderBatchID)) |>
    dplyr::left_join(dplyr::tbl(con, "ma_Grader_BatchT") |>
                dplyr::select(c(GraderBatchID, SeasonID, FarmID, BlockID)),
              by = "GraderBatchID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_FarmT") |> dplyr::select(c(FarmID, FarmName, FarmCode)),
              by = "FarmID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Farm_BlockT") |> dplyr::select(c(BlockID, BlockName, BlockCode)),
              by = "BlockID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_ProductT") |>
                dplyr::select(c(ProductID, ProductDesc, TubesPerCarton, PackTypeID,
                                TubeTypeID, PresizeFlag, SampleFlag, CountID, DesignationID, SizeID)),
              by = "ProductID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Tube_TypeT") |>
                dplyr::select(c(TubeTypeID, TubeTypeDesc, RTEConversion, TubeDiameterID, FruitPerTube)),
              by = "TubeTypeID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Pack_TypeT") |> dplyr::select(c(PackTypeID, PackTypeDesc)),
              by = "PackTypeID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_CountT") |> dplyr::select(c(CountID, CountCode)),
              by = "CountID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_DesignationT") |> dplyr::select(c(DesignationID, DesignationCode)),
              by = "DesignationID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_SizeT") |> dplyr::select(c(SizeID, SizeCode)),
              by = "SizeID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Tube_DiameterT") |> dplyr::select(c(TubeDiameterID, TubeDiameter)),
              by = "TubeDiameterID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_SeasonT") |> dplyr::select(c(SeasonID, SeasonDesc)),
                     by = "SeasonID") |>
    dplyr::select(-c(SeasonID, FarmID, BlockID, ProductID, TubeTypeID, PackTypeID, TubeDiameterID, SizeID,
                     DesignationID, CountID)) |>
    dplyr::collect()

  PresizeInput <- dplyr::tbl(con, "ma_Bin_DeliveryT") |>
    dplyr::select(BinDeliveryID, TotalWeight, PresizeProductID, SeasonID,
                  PresizeOutputFromGraderBatchID, FarmID, BlockID, PresizeFlag) |>
    dplyr::inner_join(dplyr::tbl(con, "ma_Grader_BatchT") |> dplyr::select(GraderBatchID),
               by = c("PresizeOutputFromGraderBatchID" = "GraderBatchID")) |>
    dplyr::inner_join(dplyr::tbl(con, "sw_ProductT") |> dplyr::select(c(ProductID, TubeTypeID, GradeID)),
               by = c("PresizeProductID" = "ProductID")) |>
    dplyr::left_join(dplyr::tbl(con, "sw_Tube_TypeT") |>
                dplyr::select(c(TubeTypeID, PresizeAvgTubeWeight, RTEConversion, TubeTypeDesc)),
              by = "TubeTypeID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_GradeT") |> dplyr::select(c(GradeID, JuiceFlag)),
              by = "GradeID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_FarmT") |> dplyr::select(c(FarmID, FarmName)),
              by = "FarmID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Farm_BlockT") |> dplyr::select(c(BlockID, BlockName)),
              by = "BlockID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_SeasonT") |> dplyr::select(c(SeasonID, SeasonDesc)),
                     by = "SeasonID") |>
    dplyr::select(-c(SeasonID, FarmID, BlockID, TubeTypeID)) |>
    dplyr::rename(GraderBatchID = PresizeOutputFromGraderBatchID) |>
    dplyr::collect()

  RepackInput <- dplyr::tbl(con, "ma_Repack_Input_CartonT") |>
    dplyr::select(c(FromPalletDetailID, RepackID, CartonNo)) |>
    dplyr::group_by(FromPalletDetailID, RepackID) |>
    dplyr::summarise(RepackedNoOfUnits = n(),
                     .groups = "drop") |>
    dplyr::inner_join(dplyr::tbl(con, "ma_Pallet_DetailT") |> dplyr::select(c(PalletDetailID, GraderBatchID, ProductID)),
               by = c("FromPalletDetailID" = "PalletDetailID")) |>
    dplyr::left_join(dplyr::tbl(con, "ma_Grader_BatchT") |> dplyr::select(c(GraderBatchID, SeasonID)),
              by = "GraderBatchID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_ProductT") |> dplyr::select(c(ProductID, TubesPerCarton, TubeTypeID)),
              by = "ProductID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Tube_TypeT") |> dplyr::select(c(TubeTypeID, RTEConversion)),
              by = "TubeTypeID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_SeasonT") |> dplyr::select(c(SeasonID, SeasonDesc)),
                     by = "SeasonID") |>
    dplyr::select(-c(SeasonID)) |>
    dplyr::collect()

  GraderBatch <- dplyr::tbl(con, "ma_Grader_BatchT") |>
    dplyr::select(c(GraderBatchID, SeasonID, FarmID, BlockID)) |>
    dplyr::left_join(dplyr::tbl(con, "sw_FarmT") |> dplyr::select(c(FarmID, FarmName)),
              by = "FarmID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_Farm_BlockT") |> dplyr::select(c(BlockID, BlockName)),
              by = "BlockID") |>
    dplyr::left_join(dplyr::tbl(con, "sw_SeasonT") |> dplyr::select(c(SeasonID, SeasonDesc)),
                     by = "SeasonID") |>
    dplyr::select(-c(FarmID, BlockID, SeasonID)) |>
    dplyr::collect()


  DBI::dbDisconnect(con)

# Aggregation of Tube and Export bin grower RTEs sorted by GraderBatchID

  tubeGrowerRTEs <- tubeRTEInput |>
    dplyr::filter(SeasonDesc %in% {{seasons}},
                  NoOfUnits != 0,
                  PresizeFlag == 0,
                  SampleFlag == 0) |>
    dplyr::mutate(ProductCode = stringr::str_c(DesignationCode, CountCode, TubesPerCarton, FruitPerTube, TubeDiameter, SizeCode),
                  tubeGrowerRTEs = NoOfUnits*TubesPerCarton*RTEConversion) |>
    dplyr::group_by(GraderBatchID, ProductCode) |>
    dplyr::summarise(tubeGrowerRTEs = sum(tubeGrowerRTEs, na.rm=T),
                     .groups = "drop") |>
    tidyr::pivot_wider(id_cols = c("GraderBatchID"),
                       names_from = "ProductCode",
                       values_from = "tubeGrowerRTEs") |>
    dplyr::mutate(dplyr::across(.cols = everything(), ~tidyr::replace_na(.,0)))

# summarise the tube RTE for aggregation below

  sumTubeGrowerRTEs <- tubeGrowerRTEs |>
    dplyr::rowwise() |>
    dplyr::mutate(exportBins = sum(dplyr::c_across(tidyselect::starts_with("ROC999"))),
                  japanTrayPacks = round(sum(dplyr::c_across(tidyselect::starts_with("JAP"))), 0),
                  tempZPacks = dplyr::if_else(GraderBatchID >= 2430, sum(dplyr::c_across(tidyselect::starts_with("RKT"))),0),
                  tubePacks = round(sum(dplyr::c_across(2:ncol(tubeGrowerRTEs)))-exportBins-japanTrayPacks-tempZPacks,0)) |>
    dplyr::select(c(GraderBatchID, tubePacks, exportBins, japanTrayPacks, tempZPacks))

# calculation of Pre-size grower RTEs

  presizeGrowerRTEs <- PresizeInput |>
    dplyr::filter(PresizeFlag, !JuiceFlag,
                  SeasonDesc %in% {{seasons}},
                  !(is.na(TotalWeight) & PresizeProductID == 90)) |>
    dplyr::mutate(PSgrowerRTEs = (TotalWeight/PresizeAvgTubeWeight)*RTEConversion) |>
    dplyr::group_by(GraderBatchID) |>
    dplyr::summarise(PSgrowerRTEs = round(sum(PSgrowerRTEs),0),
                     .groups = "drop")


# Calculation of repack RTEs

  repackGrowerRTEs <- RepackInput |>
    dplyr::filter(SeasonDesc %in% {{seasons}}) |>
    dplyr::mutate(repackRTEs = RepackedNoOfUnits*TubesPerCarton*RTEConversion) |>
    dplyr::group_by(GraderBatchID) |>
    dplyr::summarise(repackRTEs = round(sum(repackRTEs, na.rm=T),0),
                     .groups = "drop")


# Aggregation of grower RTEs

  growerRTEs <- GraderBatch |>
    dplyr::filter(SeasonDesc %in% {{seasons}}) |>
    dplyr::left_join(sumTubeGrowerRTEs, by = "GraderBatchID") |>
    dplyr::left_join(presizeGrowerRTEs, by = "GraderBatchID") |>
    dplyr::left_join(repackGrowerRTEs, by = "GraderBatchID") |>
    dplyr::mutate(across(.cols = c(PSgrowerRTEs, repackRTEs), ~tidyr::replace_na(., 0)))


  growerRTEsByGraderBatch <- growerRTEs |>
    dplyr::rowwise() |>
    dplyr::mutate(totalGrowerRTEs = sum(dplyr::c_across(5:10)))

  return(growerRTEsByGraderBatch)

}
