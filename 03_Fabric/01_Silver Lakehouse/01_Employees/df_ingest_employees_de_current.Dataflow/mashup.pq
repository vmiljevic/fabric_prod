[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_employees_de44_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_employees_de44_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "HR/Employee List/") and Text.Contains([Folder Path], "DE44") and not Text.Contains([Folder Path], "SF")),
  #"Added custom 1" = Table.AddColumn(#"Filtered rows", "Year", each Text.Middle([Name], 3, 4)),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Current Year", each Date.Year(DateTime.LocalNow())),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Year", type text}, {"Current Year", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 1", each [Year] = [Current Year]),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 1", {"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] ="Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32", "Column33", "Column34"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32", "Column33", "Column34"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "PersNr" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "PersNr" then "Refresh Date" else [Date modified]),
    #"Added custom 5" = Table.AddColumn(#"Added Custom4", "Year", each if  [Column1] = "PersNr" then "Year" else Text.Middle([File Name], 3, 4)),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 5", {"Content", "Name", "Date modified"}),
  #"Removed top rows" = Table.Skip(#"Removed columns", 1),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars=true]),
  #"Filtered rows 2" = Table.SelectRows(#"Promoted Headers", each ([BuKr] = "DE44")),
  #"Added custom" = Table.AddColumn(#"Filtered rows 2", "Month", each Text.Middle([File Name], 0, 2)),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"PersNr", "Local ID"}, {"BuKr", "Company Code"}, {"Kostenst.", "Cost Center Number"}, {"Kostenstelle", "Cost Center Name"}, {"Nachname", "Last Name"}, {"Vorname", "First Name"}, {"GebDatum", "Birthday Date"}, {"Beginn", "Start Date"}, {"Stelle", "Position"}, {"BsGrd", "FTE"}, {"Anzahl", "AHC"}, {"Buchungskreis", "Company Name"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Status Kundenindividuell", "PBer", "Personalbereich", "MAGrp", "Mitarbeitergruppe", "MAKrs", "Mitarbeiterkreis", "TeilBer.", "Personalteilbereich", "Art", "Tarifart", "TrfGr", "Lohnart", "Lohnart_1", "Betrag", "Währ.", "Anzahl_2", "Jahresgehalt", "Währ._3", "WoStd", "Column33", "Column34"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Local ID", type text}, {"Company Code", type text}, {"Company Name", type text}, {"Cost Center Number", type text}, {"Cost Center Name", type text}, {"Last Name", type text}, {"First Name", type text}, {"Birthday Date", type text}, {"Start Date", type text}, {"AHC", type text}, {"FTE", type text}, {"Position", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}, {"Month", type text}})
in
    #"Changed column type";
shared fact_employees_de44_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_employees_de44_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
