graderFunction <- function(seasons, password) {
  suppressPackageStartupMessages(library(tidyverse))
  suppressPackageStartupMessages(library(tidymodels))
  library(odbc)
  library(DBI)

  con <- dbConnect(odbc(),
                   Driver = "ODBC Driver 17 for SQL Server",
                   Server = "abcrepldb.database.windows.net",
                   Database = "ABCPackerRepl",
                   UID = "abcadmin",
                   PWD = password,
                   Port = 1433
  )
  #
  # the juice fruit is now being put through the Bin_Delivery table with PresizeProdutID = 278
  #
  juiceFruitKgs <- tbl(con, "ma_Bin_DeliveryT") %>%
    filter(PresizeProductID == 278) %>%
    group_by(PresizeOutputFromGraderBatchID) %>% #this is the GraderBatchID
    summarise(juiceKgs = sum(TotalWeight, na.rm=T),
              .groups = "drop") %>%
    rename(GraderBatchID = PresizeOutputFromGraderBatchID) %>%
    collect()
  #
  # Sample fruit is the undersize and oversize fruit and the sample fruit also.
  #
  sampleFruitKgs <- left_join(tbl(con, "ma_Pallet_DetailT"),
                              tbl(con, "sw_ProductT"), by = "ProductID") %>%
    filter(SampleFlag == 1) %>%
    group_by(GraderBatchID) %>%
    summarise(sampleKgs = sum(NoOfUnits*NetFruitWeight, na.rm=T)) %>%
    collect()
  #
  # Lastly the WasteOtherKgs makes up the rejectKgs.  Also the compac reconciliation
  # hence the negative numbers in some cases
  #
  GraderBatchT <- left_join(tbl(con, "ma_Grader_BatchT"),
                            tbl(con, "sw_FarmT") %>% select(c(FarmID, FarmCode, FarmName)),
                            by = "FarmID") %>%
    left_join(tbl(con, "sw_Farm_BlockT") %>% select(c(BlockID, BlockCode, BlockName)), by ="BlockID") %>%
    left_join(tbl(con, "sw_MaturityT") %>% select(c(MaturityID, MaturityCode)), by = "MaturityID") %>%
    left_join(tbl(con, "sw_CompanyT") %>% select(c(CompanyID, CompanyName)), by = c("GrowerCompanyID" = "CompanyID")) %>%
    left_join(tbl(con, "ma_ShiftT") %>% select(ShiftID, ShiftCode), by = "ShiftID") %>%
    rename(owner = CompanyName) %>%
    select(c(GraderBatchID,
             GraderBatchNo,
             SeasonID,
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
             BlockCode,
             BlockName,
             owner,
             MaturityCode)) %>%
    collect()
  #
  # looseKgs calculation
  #
  looseFruitKgs1 <- inner_join(tbl(con, "ma_Export_Bin_DetailT"),
                               tbl(con, "ma_Pallet_DetailT") %>% select(c(PalletDetailID, GraderBatchID)),
                               by = "PalletDetailID") %>%
    group_by(GraderBatchID) %>%
    summarise(looseKgs = sum(KGWeight, na.rm=T)) %>%
    collect()

  looseFruitKgs <- tbl(con, "ma_Pallet_DetailT") |>
    select(c(PalletDetailID, PalletID, ProductID, NoOfUnits, GrowerCompanyID, GraderBatchID)) |>
    left_join(tbl(con, "sw_ProductT") |> select(c(ProductID, GradeID, TubesPerCarton, NetFruitWeight)),
              by="ProductID") |>
    left_join(tbl(con, "sw_GradeT") |> select(c(GradeID, GradeDesc, PoolTypeID)),
              by="GradeID") |>
    left_join(tbl(con, "sys_fi_Pool_TypeT") |> select(c(PoolTypeID, PoolByRule)),
              by = "PoolTypeID") |>
    left_join(tbl(con, "ma_Export_Bin_DetailT") |> select(c(ExportBinDetailID, PalletDetailID, KGWeight)),
              by = "PalletDetailID") |>
    collect() |>
    filter(PoolByRule == "KG",
           is.na(ExportBinDetailID),
           GradeDesc != "Push Pack") |>
    mutate(looseKgs = NoOfUnits*NetFruitWeight) |>
    select(c(GraderBatchID, looseKgs)) %>%
    bind_rows(looseFruitKgs1) %>%
    group_by(GraderBatchID) %>%
    summarise(looseKgs = sum(looseKgs, na.rm=T),
              .groups = "drop")
#
  bin_qty <- tbl(con, "ma_Bin_UsageT") %>%
    collect() %>%
    select(GraderBatchID, BinQty) %>%
    group_by(GraderBatchID) %>%
    summarise(fieldBinsTipped = sum(BinQty, na.rm=T))

  DBI::dbDisconnect(con)

#==================================2016 data pull=================================================

  con <- dbConnect(odbc(),
                   Driver = "ODBC Driver 17 for SQL Server", #"SQLServer", #
                   Server = "abcrepldb.database.windows.net",
                   Database = "ABCPackRepl",
                   UID = "abcadmin",
                   PWD = password,
                   Port = 1433
  )

  GraderBatchT2019 <- left_join(tbl(con, "ma_Grader_BatchT"),
                                tbl(con, "sw_FarmT") %>% select(c(FarmID, FarmCode, FarmName, GrowerCompanyID)),
                                by = "FarmID") %>%
    left_join(tbl(con, "sw_CompanyT") %>% select(CompanyID, CompanyName),
              by = c("GrowerCompanyID" = "CompanyID")) %>%
    rename(owner = CompanyName) %>%
    mutate(ShiftCode = "Day") %>%
    select(c(GraderBatchID,
             GraderBatchNo,
             SeasonID,
             PackDate,
             InputKgs,
             ShiftCode,
             RejectKgs,
             owner,
             FarmCode,
             FarmName)) %>%
    collect()

  Bins2019 <- tbl(con, "ma_Bin_UsageT") %>%
    group_by(GraderBatchID) %>%
    summarise(BinID = max(BinID, na.rm=T),
              BinQty = sum(BinQty, na.rm=T),
              .groups = "drop") %>%
    left_join(tbl(con, "ma_BinT") %>%
                select(c(BinID, SeasonID, FarmID, BlockID, NoOfBins, HarvestDate,
                         StorageTypeID, PresizeFlag, FirstStorageSiteCompanyID, ESPID)),
              by = "BinID")%>%
    left_join(tbl(con, "sw_Farm_BlockT") %>% select(c(BlockID, BlockCode, BlockName)),
              by = "BlockID") %>%
    left_join(tbl(con, "sw_Storage_TypeT") %>% select(c(StorageTypeID, StorageTypeDesc)),
              by = "StorageTypeID") %>%
    left_join(tbl(con, "sw_CompanyT") %>% select(c(CompanyID, CompanyName)),
              by = c("FirstStorageSiteCompanyID" = "CompanyID")) %>%
    left_join(tbl(con, "sw_ESPT") %>% select(c(ESPID, ESPCode)),
              by = "ESPID") %>%
    mutate(StorageSite = case_when(CompanyName == "Rockit Packing Company Ltd" ~ "Cooper Street",
                                   CompanyName == "Berl Property Ltd" ~ "Raupare Rd",
                                   CompanyName == "Bostock New Zealand" ~ "Henderson Road",
                                   CompanyName == "T & G (Apollo)" ~ "T&G Whakatu",
                                   CompanyName == "Everest Kool SolutioNZ" ~ "Everest Kool Omahu Rd",
                                   CompanyName == "Crasborn Fresh Harvest " ~ "FreshMax Omahu Road",
                                   CompanyName == "2019 Crasborn Fresh Harvest" ~ "Raupare Rd",
                                   TRUE ~ CompanyName)) %>%
    select(-c(FarmID, BlockID, StorageTypeID, FirstStorageSiteCompanyID, ESPID, CompanyName )) %>%
    arrange(GraderBatchID) %>%
    collect()

  dbDisconnect(con)

#==================================aggregation==================================================

    graderBatchData2020 <- left_join(GraderBatchT, juiceFruitKgs,
                               by = "GraderBatchID") %>%
    left_join(sampleFruitKgs, by="GraderBatchID") %>%
    left_join(looseFruitKgs, by = "GraderBatchID") %>%
    mutate(across(.cols = c(juiceKgs, sampleKgs, looseKgs), ~replace_na(.x,0))) %>%
    left_join(bin_qty, by = "GraderBatchID") %>%
    filter(!is.na(WasteOtherKgs),
           PresizeInputFlag == 0) %>%
    mutate(InputKgs = InputKgs,
           rejectKgs = WasteOtherKgs + juiceKgs + sampleKgs + looseKgs,
           GraderBatchNo = as.integer(GraderBatchNo),
           packOut = 1-rejectKgs/InputKgs,
           storageDays = as.numeric(PackDate - HarvestDate),
           StorageType = case_when(
             StorageTypeID == 4 ~ "CA",
             StorageTypeID == 7 ~ "RA",
             TRUE ~ "RA"),
           Season = case_when(
             SeasonID == 6 ~ 2020,
             SeasonID == 7 ~ 2021,
             TRUE ~ 2022)) %>%
    mutate(owner = str_trim(owner, side = "both"),
           owner = str_to_lower(owner),
           owner = str_replace_all(owner, " ", "")) %>%
    mutate(ifelse(storageDays < 50 & StorageType == "CA", "RA", "RA")) %>%
    select(GraderBatchID,
           GraderBatchNo,
           Season,
           owner,
           FarmCode,
           FarmName,
           BlockCode,
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

  graderBatchData2019 <- GraderBatchT2019 %>%
    left_join(Bins2019 %>% filter(!is.na(GraderBatchID)) %>%
                select(-c(SeasonID, BinID)),
              by = "GraderBatchID") %>%
    rename(rejectKgs = RejectKgs) %>%
    mutate(Season = case_when(SeasonID == 2 ~ 2016,
                              SeasonID == 3 ~ 2017,
                              SeasonID == 4 ~ 2018,
                              TRUE ~ 2019),
           StorageType = case_when(StorageTypeDesc == "Normal Air" ~ "RA",
                                   StorageTypeDesc == "CA & Smartfreshed" ~ "CA",
                                   TRUE ~ "RA"),
           packOut = 1-rejectKgs/InputKgs,
           storageDays = as.numeric(PackDate - HarvestDate),
           GraderBatchNo = as.integer(GraderBatchNo)) %>%
    mutate(owner = str_trim(owner, side = "both"),
           owner = str_to_lower(owner),
           owner = str_replace_all(owner, " ", "")) %>%
    select(-c(SeasonID,StorageTypeDesc, NoOfBins)) %>%
    rename(MaturityCode = ESPCode,
           fieldBinsTipped = BinQty)

  gbd <- graderBatchData2019 %>%
    select(c(GraderBatchID,
             GraderBatchNo,
             Season,
             owner,
             FarmCode,
             FarmName,
             BlockCode,
             HarvestDate,
             PackDate,
             ShiftCode,
             storageDays,
             fieldBinsTipped,
             InputKgs,
             rejectKgs,
             packOut,
             MaturityCode,
             StorageType)) %>%
    bind_rows(graderBatchData2020) %>%
    filter(Season %in% {{seasons}})

  return(gbd)
}

#============================================================================================================================

binsHarvested <- function(seasons, password) {
  suppressPackageStartupMessages(library(tidyverse))
  suppressPackageStartupMessages(library(tidymodels))
  library(odbc)
  library(DBI)

  con <- DBI::dbConnect(odbc::odbc(),
                   Driver = "ODBC Driver 17 for SQL Server",
                   Server = "abcrepldb.database.windows.net",
                   Database = "ABCPackerRepl",
                   UID = "abcadmin",
                   PWD = password,
                   Port = 1433
  )

  binsCompany2020 <- tbl(con, "ma_Bin_DeliveryT") %>%
    filter(PresizeFlag == 0) %>%
    dplyr::select(BinDeliveryID, SeasonID,
                  FirstStorageSiteCompanyID) %>%
    left_join(tbl(con, "sw_CompanyT") %>% select(c(CompanyID, CompanyName)),
              by = c("FirstStorageSiteCompanyID" = "CompanyID")) %>%
    mutate(StorageSite = case_when(CompanyName == "Te Ipu Packhouse (RO)" & SeasonID > 6 ~ "Te Ipu",
                                   CompanyName == "Te Ipu Packhouse (RO)" & SeasonID <= 6 ~ "Cooper Street",
                                   CompanyName == "Berl Property Ltd" ~ "Raupare Rd",
                                   TRUE ~ "Raupare Rd")) %>%
    dplyr::select(-c(SeasonID, CompanyName, FirstStorageSiteCompanyID)) %>%
    collect()

  binsHarvested2020 <- tbl(con, "ma_Bin_DeliveryT") %>%
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
                  GrowingTypeID) %>%
    left_join(tbl(con, "sw_Farm_BlockT") %>% select(BlockID, BlockCode, BlockName),
              by="BlockID") %>%
    left_join(tbl(con, "sw_FarmT") %>% select(c(FarmID, FarmCode, FarmName, GrowerCompanyID)),
              by = "FarmID") %>%
    left_join(tbl(con, "sw_CompanyT") %>% select(c(CompanyID, CompanyName)),
              by = c("GrowerCompanyID" = "CompanyID")) %>%
    rename(owner = CompanyName) %>%
    left_join(tbl(con, "sw_Pick_NoT") %>% select(c(PickNoID, PickNoDesc)),
              by = "PickNoID") %>%
    left_join(tbl(con, "sw_TreatmentT") %>% select(c(TreatmentID, TreatmentDesc)),
              by = "TreatmentID") %>%
    left_join(tbl(con, "sw_TagT") %>% select(c(TagID, TagDesc)),
              by = "TagID") %>%
    left_join(tbl(con, "sw_Growing_TypeT") %>% select(c(GrowingTypeID, GrowingTypeDesc)),
              by = "GrowingTypeID") %>%
    left_join(tbl(con, "sw_MaturityT") %>% select(c(MaturityID, MaturityCode)),
              by = "MaturityID") %>%
    mutate(Season = case_when(SeasonID == 6 ~ 2020,
                              SeasonID == 7 ~ 2021,
                              SeasonID == 8 ~ 2022),
           StorageType = case_when(StorageTypeID == 4 ~ "CA",
                                   StorageTypeID == 7 ~ "RA",
                                   TRUE ~ "RA")) %>%
    filter(PresizeFlag == 0) %>%
    rename(ConsignmentNumber = BinDeliveryNo) %>%
    select(-c(SeasonID,
              PresizeFlag,
              FarmID,
              BlockID,
              StorageTypeID,
              PickNoID,
              TreatmentID,
              TagID,
              GrowingTypeID,
              GrowerCompanyID,
              MaturityID)) %>%
    relocate(Season, .after = ConsignmentNumber) %>%
    collect() %>%
    mutate(owner = str_trim(owner, side = "both"),
           owner = str_to_lower(owner),
           owner = str_replace_all(owner, " ", ""))

  DBI::dbDisconnect(con)

  bh2020 <- binsHarvested2020 |>
    left_join(binsCompany2020, by = "BinDeliveryID")

  con <- DBI::dbConnect(odbc::odbc(),
                   Driver = "ODBC Driver 17 for SQL Server",
                   Server = "abcrepldb.database.windows.net",
                   Database = "ABCPackRepl",
                   UID = "abcadmin",
                   PWD = password,
                   Port = 1433
  )

  binsCompany2019 <- tbl(con, "ma_BinT") %>%
    filter(PresizeFlag == 0) %>%
    select(c(BinID, FirstStorageSiteCompanyID))%>%
    left_join(tbl(con, "sw_CompanyT") %>% select(c(CompanyID, CompanyName)),
              by = c("FirstStorageSiteCompanyID" = "CompanyID")) %>%
    mutate(StorageSite = case_when(CompanyName == "Rockit Packing Company Ltd" ~ "Cooper Street",
                                   CompanyName == "Berl Property Ltd" ~ "Raupare Rd",
                                   CompanyName == "Bostock New Zealand" ~ "Henderson Road",
                                   CompanyName == "T & G (Apollo)" ~ "T&G Whakatu",
                                   CompanyName == "Everest Kool SolutioNZ" ~ "Everest Kool Omahu Rd",
                                   CompanyName == "Crasborn Fresh Harvest " ~ "FreshMax Omahu Road",
                                   CompanyName == "2019 Crasborn Fresh Harvest" ~ "Raupare Rd",
                                   TRUE ~ CompanyName)) %>%
    dplyr::select(-c(FirstStorageSiteCompanyID, CompanyName)) %>%
    rename(BinDeliveryID = BinID) |>
    collect()

  binsHarvested2019 <- tbl(con, "ma_BinT") %>%
    filter(PresizeFlag == 0) %>%
    select(c(BinID, BinNo, SeasonID, HarvestDate, NoOfBins, FarmID, BlockID,
             StorageTypeID, PresizeFlag, FirstStorageSiteCompanyID, Comment,
             ESPID, BinReceivedDate))%>%
    left_join(tbl(con, "sw_FarmT") %>% select(FarmID, FarmCode, FarmName, GrowerCompanyID),
              by = "FarmID") %>%
    left_join(tbl(con, "sw_Farm_BlockT") %>% select(c(BlockID, BlockCode, BlockName)),
              by = "BlockID") %>%
    left_join(tbl(con, "sw_CompanyT") %>% select(c(CompanyID, CompanyName)),
              by = c("GrowerCompanyID" = "CompanyID")) %>%
    rename(owner = CompanyName) %>%
    left_join(tbl(con, "sw_ESPT") %>% select(c(ESPID, ESPCode)),
              by = "ESPID") %>%
    mutate(Season = case_when(SeasonID == 2 ~ 2016,
                              SeasonID == 3 ~ 2017,
                              SeasonID == 4 ~ 2018,
                              SeasonID == 5 ~ 2019),
           StorageType = case_when(StorageTypeID == 4 ~ "CA",
                                   StorageTypeID == 7 ~ "RA",
                                   TRUE ~ "RA")) %>%
    dplyr::select(-c(FarmID, BlockID, StorageTypeID, SeasonID, ESPID,
              PresizeFlag, GrowerCompanyID)) %>%
    rename(BinDeliveryID = BinID,
           ConsignmentNumber = BinNo,
           ReceivedDate = BinReceivedDate,
           MaturityCode = ESPCode) %>%
    mutate(ReceivedTime = as.character(NA),
           PickNoDesc = as.character(NA),
           TreatmentDesc = as.character(NA),
           TagDesc = as.character(NA),
           GrowingTypeDesc = as.character(NA)) %>%
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
             owner,
             PickNoDesc,
             TreatmentDesc,
             TagDesc,
             GrowingTypeDesc,
             MaturityCode,
             StorageType)) %>%
    collect() %>%
    mutate(owner = str_trim(owner, side = "both"),
           owner = str_to_lower(owner),
           owner = str_replace_all(owner, " ", ""))

  DBI::dbDisconnect(con)

  bh2019 <- binsHarvested2019 |>
    left_join(binsCompany2019, by = "BinDeliveryID")

  binsHarvested <- bh2019 %>%
    bind_rows(bh2020) %>%
    filter(Season %in% {{seasons}})

  return(binsHarvested)
}








