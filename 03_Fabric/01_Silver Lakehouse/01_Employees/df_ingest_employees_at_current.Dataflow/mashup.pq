[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_employees_at45_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_employees_at45_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "HR/Employee List/") and Text.Contains([Folder Path], "AT45") and not Text.Contains([Folder Path], "SF")),
  #"Added custom 1" = Table.AddColumn(#"Filtered rows", "Year", each Text.Middle([Name], 3, 4)),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Current Year", each Date.Year(DateTime.LocalNow())),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Year", type text}, {"Current Year", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 1", each [Year] = [Current Year]),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 1", {"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] ="MitarbeiterInnen")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data 1" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32", "Column33", "Column34", "Column35", "Column36", "Column37", "Column38", "Column39", "Column40", "Column41", "Column42", "Column43", "Column44", "Column45", "Column46", "Column47", "Column48"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32", "Column33", "Column34", "Column35", "Column36", "Column37", "Column38", "Column39", "Column40", "Column41", "Column42", "Column43", "Column44", "Column45", "Column46", "Column47", "Column48"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data 1", "File Name", each if [Column2] = "NACHNAME" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column2] = "NACHNAME" then "Refresh Date" else [Date modified]),
    #"Added custom 5" = Table.AddColumn(#"Added Custom4", "Year", each if  [Column2] = "NACHNAME" then "Year" else Text.Middle([File Name], 3, 4)),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 5", {"Content", "Name", "Date modified"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Filtered rows 3" = Table.SelectRows(#"Promoted Headers", each ([PERNR] <> "PERNR" and [PERNR] <> null) and ([BK] = 45)),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 3", {"WERKTXT", "Appr", "Interns", "SalesF", "STEL", "AZGRADE", "LEVEL", "HC", "BK", "BKAHC", "WERK", "PERSK", "ID", "AD", "RESSORT", "ORGEH", "ZAUSTRITT", "ATZVON", "ATZVOLLBIS", "BT","AWART", "MSTBR", "BSGRD", "DefAb", "Hauptbeleihung", "TITELINT", "TELE"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"PERNR", "Local ID"}, {"NACHNAME", "Last Name"}, {"VORNAME", "First Name"}, {"TITELAKAD", "Title"}, {"KOST", "Cost Center Number"}, {"KOSTTXT", "Cost Center Name"}, {"BKTXT", "Company Name"},{"EMAIL", "Email"}, {"USERID", "User ID"}, {"GESCHL", "Gender"}, {"STELTXT", "Position"}, {"ORGEHTXT", "Role Type"},{"BTTXT", "Location"},{"EINTRITT", "Start Date"}, {"AUSTRITT", "End Date"}, {"PernrVorg", "HR Manager Personal Number"}, {"NameVorg", "HR Manager"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "Month", each Text.Middle([File Name], 0, 2)),
  #"Added custom 3" = Table.AddColumn(#"Added custom", "Company Code", each "AT45"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 3", {{"Local ID", type text}, {"Last Name", type text}, {"First Name", type text}, {"Title", type text}, {"AHC", type text}, {"FTE", type text}, {"Cost Center Number", type text}, {"Cost Center Name", type text}, {"Position", type text}, {"Company Name", type text}, {"Role Type", type text}, {"Location", type text}, {"Start Date", type text}, {"End Date", type text}, {"Gender", type text}, {"User ID", type text}, {"Email", type text}, {"HR Manager Personal Number", type text}, {"HR Manager", type text}, {"DisciplinaryOrgUnit", type text}, {"FunctionalOrgUnit", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}, {"Month", type text}, {"Company Code", type text}})
in
    #"Changed column type";
shared fact_employees_at45_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_employees_at45_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
