[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_time_recording_hr_tele_at45_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_time_recording_hr_tele_at45 = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/HR/Time%20Booking/AT45/Telearbeit_AZTECH_2024_V1.xlsx"), null, true),
    Tabelle1_Sheet = Source{[Item="Tabelle1",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Tabelle1_Sheet, [PromoteAllScalars=true]),
 #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"UhrzeitBeginn", type text}}),
    #"Trimmed Text" = Table.TransformColumns(#"Changed Type",{{"UhrzeitBeginn", Text.Trim, type text}}),
    #"Added Custom" = Table.AddColumn(#"Trimmed Text", "UhrzeitBeginCorrected", each Text.PadStart([UhrzeitBeginn],6,"0")),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom",{{"UhrzeitBeginCorrected", type time}, {"UhrzeitEnde", type text}}),
    #"Trimmed Text1" = Table.TransformColumns(#"Changed Type1",{{"UhrzeitEnde", Text.Trim, type text}}),
    #"Added Custom1" = Table.AddColumn(#"Trimmed Text1", "UhrzeitEndeCorrected", each Text.PadStart([UhrzeitEnde],6,"0")),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom1",{{"UhrzeitEndeCorrected", type time}}),
    #"Replaced Errors" = Table.ReplaceErrorValues(#"Changed Type2", {{"UhrzeitEndeCorrected", #time(23, 59, 59)}}),
    #"Added Custom6" = Table.AddColumn(#"Replaced Errors", "Time Difference 2", each if (([UhrzeitEndeCorrected]-[UhrzeitBeginCorrected]) <> null and [UhrzeitEndeCorrected] > [UhrzeitBeginCorrected]) then (Number.From([UhrzeitEndeCorrected]-[UhrzeitBeginCorrected])*24) else [Stundenanzahl]),
    #"Added Custom7" = Table.AddColumn(#"Added Custom6", "Time Difference incl. Break", each if ([Time Difference 2] > 5) then ([Time Difference 2] - 0.5) else [Time Difference 2]),
    #"Rounded Off1" = Table.TransformColumns(#"Added Custom7",{{"Time Difference incl. Break", each Number.Round(_, 2), type number}}),
    #"Changed Type4" = Table.TransformColumnTypes(#"Rounded Off1",{{"Stundenanzahl", type number}}),
    #"Added Custom4" = Table.AddColumn(#"Changed Type4", "SAP Capacity", each [Time Difference incl. Break]/8),
    #"Changed Type5" = Table.TransformColumnTypes(#"Added Custom4",{{"SAP Capacity", type number}}),
    #"Added Custom5" = Table.AddColumn(#"Changed Type5", "Month Date", each Text.Middle(Date.MonthName([DatumBeginn],"en-US"),0,3) & " "& Text.From(Date.Year([DatumBeginn]))),
    #"Changed Type6" = Table.TransformColumnTypes(#"Added Custom5",{{"PERNR", type text}}),
    #"Added Custom2" = Table.AddColumn(#"Changed Type6", "SAP Capacity Check", each [Stundenanzahl]/8),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom2",{{"SAP Capacity Check", type number}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type3",{{"PERNR", "Personal Number"}, {"Nachname", "Last Name"}, {"Vorname", "First Name"}}),
    #"Grouped Rows" = Table.Group(#"Renamed Columns", {"Personal Number", "First Name", "Last Name", "Month Date"}, {{"Booked in SAP", each List.Sum([SAP Capacity]), type nullable number}}),
    #"Changed Type7" = Table.TransformColumnTypes(#"Grouped Rows",{{"Month Date", type date}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type7", each ([Personal Number] <> null)),
    #"Changed Type8" = Table.TransformColumnTypes(#"Filtered Rows",{{"Personal Number", Int64.Type}}),
  #"Added custom 1" = Table.AddColumn(#"Changed Type8", "Company Code", each "AT45"),
  #"Renamed columns 1" = Table.RenameColumns(#"Added custom 1", {{"Personal Number", "HR ID"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"HR ID", type text}, {"First Name", type text}, {"Last Name", type text}, {"Month Date", type text}, {"Booked in SAP", type text}, {"Company Code", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_time_recording_hr_corr_at45_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_time_recording_hr_corr_at45 = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/HR/Time%20Booking/AT45/Zeitbuchungen_AZTECH_2024_V1.xlsx"), null, true),
    Tabelle1_Sheet = Source{[Item="Tabelle1",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Tabelle1_Sheet, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"PERNR", Int64.Type}, {"Datum", type date}, {"monat", Int64.Type}, {"jahr", Int64.Type}, {"mt", type number}, {"Kommen", type time}, {"Gehen", type time}, {"StundenInklPause", type number}, {"NACHN", type text}, {"VORNA", type text}, {"KOSTL", Int64.Type}, {"KOTXT", type text}, {"STELL", Int64.Type}, {"STLTX", type text}}),
    #"Added Custom" = Table.AddColumn(#"Changed Type", "Time", each if [Gehen] > [Kommen] then Number.From([Gehen]-[Kommen])*24 else [StundenInklPause]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom", "Time incl. Break", each if [Time] > 5 then [Time]-0.5 else [Time]),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom4",{{"Time incl. Break", type number}}),
    #"Rounded Off" = Table.TransformColumns(#"Changed Type1",{{"Time incl. Break", each Number.Round(_, 2), type number}}),
    #"Added Custom1" = Table.AddColumn(#"Rounded Off", "SAP Capacity", each [Time incl. Break]/8),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom1",{{"SAP Capacity", type number}}),
    #"Added Custom2" = Table.AddColumn(#"Changed Type2", "Month Date", each Text.Middle(Date.MonthName([Datum],"en-US"),0,3) & " "& Text.From(Date.Year([Datum]))),
    #"Changed Type5" = Table.TransformColumnTypes(#"Added Custom2",{{"Month Date", type date}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type5",{{"PERNR", "Personalnummer"}}),
    #"Changed Type3" = Table.TransformColumnTypes(#"Renamed Columns",{{"Personalnummer", type text}}),
    #"Added Custom3" = Table.AddColumn(#"Changed Type3", "SAP Capacity Check", each [StundenInklPause]/8),
    #"Changed Type4" = Table.TransformColumnTypes(#"Added Custom3",{{"SAP Capacity Check", type number}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Changed Type4",{{"Personalnummer", "Personal Number"}, {"jahr", "Year"}, {"NACHN", "Last Name"}, {"VORNA", "First Name"}}),
    #"Grouped Rows" = Table.Group(#"Renamed Columns1", {"Personal Number", "First Name", "Last Name", "Month Date"}, {{"Booked in SAP", each List.Sum([SAP Capacity]), type nullable number}}),
    #"Changed Type6" = Table.TransformColumnTypes(#"Grouped Rows",{{"Personal Number", Int64.Type}}),
  #"Added custom 1" = Table.AddColumn(#"Changed Type6", "Company Code", each "AT45"),
  #"Renamed columns 1" = Table.RenameColumns(#"Added custom 1", {{"Personal Number", "HR ID"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"HR ID", type text}, {"First Name", type text}, {"Last Name", type text}, {"Month Date", type text}, {"Booked in SAP", type text}, {"Company Code", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_time_recording_hr_ce_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_time_recording_hr_ce = let
    // --- Load and filter files only once ---
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
    #"Filtered rows" = Table.SelectRows(Source,
        each Text.Contains([Folder Path], "HR/Time Booking/")
            and not Text.Contains([Folder Path], "AT45")
         
    ),
    #"Added custom 1" = Table.AddColumn(#"Filtered rows", "Year", each Text.Middle([Name], 0, 4)),
    #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Current Year", each Date.Year(DateTime.LocalNow())),
    #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Year", type text}, {"Current Year", type text}}),
    #"Removed Other Columns4" = Table.SelectColumns(#"Changed column type 1", { "Name", "Date modified","Content"}),

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
  #"Expanded Extracted Data" = Table.ExpandTableColumn(#"Removed columns", "Extracted Data", {"HR Number", "Working Hours", "Overtime Hours", "Month"}, {"HR Number", "Working Hours", "Overtime Hours", "Month"}),
    #"Added custom 5" = Table.AddColumn(#"Expanded Extracted Data", "Year", each Text.Middle([Name], 0, 4)),
  #"Renamed columns" = Table.RenameColumns(#"Added custom 5", {{"Name", "File Name"}, {"Date modified", "Refresh Date"}, {"HR Number", "HR ID"}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Renamed columns", "File Name", "File Name - Copy"),
  #"Split column by delimiter" = Table.SplitColumn(#"Duplicated column", "File Name", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv, true), {"File Name.1", "File Name.2"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Split column by delimiter", {"File Name.1"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns 1", ".xlsx", "", Replacer.ReplaceText, {"File Name.2"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Replaced value", {{"File Name.2", "Company Code"}, {"File Name - Copy", "File Name"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"Company Code", type text}, {"Refresh Date", type text}, {"HR ID", type text}, {"Working Hours", type text}, {"Overtime Hours", type text}, {"Month", type text}, {"Year", type text}, {"File Name", type text}})
in
    #"Changed column type";
shared fact_time_recording_hr_tele_at45_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_time_recording_hr_tele_at45", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_time_recording_hr_corr_at45_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_time_recording_hr_corr_at45", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_time_recording_hr_ce_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_time_recording_hr_ce", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
