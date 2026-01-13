[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_employees_ce_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_employees_ce_archive = let
    // --- Load and filter files only once ---
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
    #"Filtered rows" = Table.SelectRows(Source,
        each Text.Contains([Folder Path], "HR/Employee List/")
            and not Text.Contains([Folder Path], "AT45")
            and not Text.Contains([Folder Path], "DE44")
    ),
    #"Added custom 1" = Table.AddColumn(#"Filtered rows", "Year", each Text.Middle([Name], 3, 4)),
    #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Current Year", each Date.Year(DateTime.LocalNow())),
    #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Year", type text}, {"Current Year", type text}}),
    #"Filtered rows 1" = Table.SelectRows(#"Changed column type 1", each [Year] < [Current Year]),
  #"Filtered rows 3" = Table.SelectRows(#"Filtered rows 1", each ([Year] = "2024")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 3", { "Name", "Date modified","Content"}),

    // --- Read first sheet from each file ---
    #"Added Custom1" = Table.AddColumn(
        #"Removed Other Columns4",
        "Extracted Data",
        each
            let
                wb = Excel.Workbook([Content], true),
                firstSheet = Table.SelectRows(wb, each [Kind] = "Sheet"){0}[Data]
            in
                firstSheet
    ),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom1", {"Content"}),
  #"Expanded Extracted Data" = Table.ExpandTableColumn(#"Removed columns", "Extracted Data", {"User ID", "HR Number", "EC ID", "Last Name", "First Name", "Head Count", "Active Head Count", "Full time Employee", "Company Code", "Cost Center", "Cost Center Name", "Position", "Company Name", "Team Name", "City", "Start Date of employment", "End Date of Employment", "Gender", "Email", "HR Manager Name", "Date of Birth", "Rate Card", "Invoicing Method"}, {"User ID", "HR Number", "EC ID", "Last Name", "First Name", "Head Count", "Active Head Count", "Full time Employee", "Company Code", "Cost Center", "Cost Center Name", "Position", "Company Name", "Team Name", "City", "Start Date of employment", "End Date of Employment", "Gender", "Email", "HR Manager Name", "Date of Birth", "Rate Card", "Invoicing Method"}),
    #"Added custom 5" = Table.AddColumn(#"Expanded Extracted Data", "Year", each Text.Middle([Name], 3, 4)),
  #"Added custom 3" = Table.AddColumn(#"Added custom 5", "Month", each Text.Middle([Name], 0, 2)),
  #"Filtered rows 2" = Table.SelectRows(#"Added custom 3", each ([Last Name] <> null)),
  #"Renamed columns" = Table.RenameColumns(#"Filtered rows 2", {{"Name", "File Name"}, {"Date modified", "Refresh Date"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"File Name", type text}, {"Refresh Date", type text}, {"User ID", type text}, {"HR Number", type text}, {"EC ID", type text}, {"Last Name", type text}, {"First Name", type text}, {"Head Count", type text}, {"Active Head Count", type text}, {"Full time Employee", type text}, {"Company Code", type text}, {"Cost Center", type text}, {"Cost Center Name", type text}, {"Position", type text}, {"Company Name", type text}, {"Team Name", type text}, {"City", type text}, {"Start Date of employment", type text}, {"End Date of Employment", type text}, {"Gender", type text}, {"Email", type text}, {"HR Manager Name", type text}, {"Date of Birth", type text}, {"Rate Card", type text}, {"Invoicing Method", type text}, {"Year", type text}, {"Month", type text}})
in
    #"Changed column type";
shared fact_employees_ce_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_employees_ce_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
