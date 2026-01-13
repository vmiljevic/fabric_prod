[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cost_elements_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_cost_element_current = let
    Source = SharePoint.Tables("https://allianzms.sharepoint.com/teams/DE1214-4322362/", [Implementation="2.0", ViewMode="All"]),
    #"b3093836-4a96-4033-8782-71e92faa8b61" = Source{[Id="b3093836-4a96-4033-8782-71e92faa8b61"]}[Items],
    #"Filtered Rows1" = Table.SelectRows(#"b3093836-4a96-4033-8782-71e92faa8b61", each true),
    #"Renamed Columns" = Table.RenameColumns(#"Filtered Rows1",{{"Title", "Cost Element"}, {"CostElement name", "Cost Element Name"}}),
    #"Expanded CostElements-Group" = Table.ExpandListColumn(#"Renamed Columns", "CostElements-Group"),
    #"Expanded CostElements-Group1" = Table.ExpandRecordColumn(#"Expanded CostElements-Group", "CostElements-Group", {"lookupValue"}, {"CostElements-Group.lookupValue"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Expanded CostElements-Group1",{{"CostElements-Group.lookupValue", "Cost Element Group"}, {"AMOS Rep L3:Amos-Rep L3 Name", "L3 Name"}}),
    #"Expanded AMOS Rep L3" = Table.ExpandListColumn(#"Renamed Columns1", "AMOS Rep L3"),
    #"Expanded AMOS Rep L1" = Table.ExpandRecordColumn(#"Expanded AMOS Rep L3", "AMOS Rep L3", {"lookupValue"}, {"AMOS Rep L3.lookupValue"}),
    #"Renamed Columns2" = Table.RenameColumns(#"Expanded AMOS Rep L1",{{"AMOS Rep L3.lookupValue", "L3 Code"}, {"AMOS Rep L3:Amos-Rep L2 Code", "L2 Code"}, {"AMOS Rep L3:Amos-Rep L2 Name", "L2 Name"}, {"AMOS Rep L3:Amos-Rep L1 Code", "L1 Code"}, {"AMOS Rep L3:Amos-Rep L1 Name", "L1 Name"}}),
    #"Removed Other Columns" = Table.SelectColumns(#"Renamed Columns2",{"Cost Element", "Cost Element Name", "Cost Element Group", "KSC_CRV_FC", "CostElements-Group:Prim-Sec", "L3 Code", "L3 Name", "L2 Code", "L2 Name", "L1 Code", "L1 Name"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Other Columns", each ([L3 Name] <> null)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered Rows", {"KSC_CRV_FC", "CostElements-Group:Prim-Sec"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed columns", each ([Cost Element] <> "not definfed Component")),
  #"Added custom" = Table.AddColumn(#"Filtered rows 1", "Type", each if Text.Contains([L1 Name],"Revenue", Comparer.OrdinalIgnoreCase) = true then "Revenue" else "Costs"),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Cost Element and Name", each [Cost Element] & " - " & [Cost Element Name]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 1", {{"Cost Element", type text}, {"Cost Element Name", type text}, {"Cost Element Group", type text}, {"L3 Code", type text}, {"L3 Name", type text}, {"L2 Code", type text}, {"L2 Name", type text}, {"L1 Code", type text}, {"L1 Name", type text}, {"Type", type text}, {"Cost Element and Name", type text}})
in
    #"Changed column type";
shared dim_cost_elements_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cost_element_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_company_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_company = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
    Company_Table = Source{[Item="CompanyCodes",Kind="Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(Company_Table, {{"Column1", "Debtor Number and Name"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Company Code", type text}, {"Company Name", type text}, {"Debtor Number", type text}, {"Debtor", type text}, {"Debtor Number and Name", type text}, {"Country", type text}})
in
    #"Changed column type";
shared dim_company_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_company", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sec_user_right_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared sec_user_right = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
    UserRights_Table = Source{[Item="UserRightsPerCountry",Kind="Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(UserRights_Table, {{"User", type text}, {"Flag", type text}, {"Role", type text}, {"Location", type text}, {"Employee lists", type text}, {"PPM Time Bookings", type text}, {"Cost & Revenue", type text}, {"Service Spend", type text}, {"Budget Availability", type text}, {"Account Receivables", type text}})
in
    #"Changed column type";
shared sec_user_right_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "sec_user_right", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared _map_organizational_unit_last_month = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/DE1214-7332185/", [ApiVersion = 15]),
    #"Filtered Rows3" = Table.SelectRows(Source, each ([Folder Path] <> null)),
    #"Added Custom14" = Table.AddColumn(#"Filtered Rows3", "FileDate", each Text.Middle([Name],0,8)),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom14",{{"FileDate", type date}}),
    #"Inserted End of Month" = Table.AddColumn(#"Changed Type2", "End of Month", each Date.EndOfMonth([FileDate])),
    #"Inserted Date" = Table.AddColumn(#"Inserted End of Month", "Date", each DateTime.Date([End of Month]), type date),
    #"Added Custom3" = Table.AddColumn(#"Inserted Date", "Custom", each DateTime.Date( DateTime.LocalNow())),
    #"Added Custom2" = Table.AddColumn(#"Added Custom3", "Custom.1", each DateTime.Date( DateTime.LocalNow())),
    #"Renamed Columns" = Table.RenameColumns(#"Added Custom2",{{"Date", "Date of Organization"}, {"Custom", "Date of HR"}}),
    #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns",{"End of Month"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns",{{"Date of HR", type date}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Changed Type",{{"Date of HR", "Date of HR End"}, {"Custom.1", "Date of HR Start"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns1",{{"Date of HR End", type date}}),
    #"Calculated Start of Month" = Table.TransformColumns(#"Changed Type1",{{"Date of HR Start", Date.StartOfMonth, type date}}),
        #"Added Custom4" = Table.AddColumn(#"Calculated Start of Month", "Check", each if [Date of Organization] >= Date.AddDays([Date of HR Start], -1) and [Date of Organization] < [Date of HR End] then 1 else 0),
    #"Filtered Rows2" = Table.SelectRows(#"Added Custom4", each ([Check] = 1)),
    #"Added Custom1" = Table.AddColumn(#"Filtered Rows2", "Extracted Data", each 
        let
            ExcelData = Excel.Workbook([Content], null, true),
            FirstSheet = ExcelData{0}[Data] // Access the first sheet by index (0-based index)
        in
            FirstSheet
    ),
    #"Added Custom" = Table.AddColumn(#"Added Custom1", "Service Month", each Date.ToText([Date of Organization],"yyyy","en-US") &Date.ToText([Date of Organization],"MM","en-US")),
    #"Filtered Rows" = Table.SelectRows(#"Added Custom", each not Text.Contains([Name], "Test")),
    #"Sorted Rows" = Table.Sort(#"Filtered Rows",{{"Date created", Order.Ascending}}),
    #"Removed Duplicates" = Table.Distinct(#"Sorted Rows", {"Service Month"}),
    #"Added Custom5" = Table.AddColumn(#"Removed Duplicates", "Custom", each Date.ToText(DateTime.Date( DateTime.LocalNow()),"yyyy","en-US")),
    #"Renamed Columns2" = Table.RenameColumns(#"Added Custom5",{{"Custom", "Current Year"}}),
    #"Added Custom6" = Table.AddColumn(#"Renamed Columns2", "File Year", each Date.ToText([Date of Organization],"yyyy","en-US")),
    #"Added Custom7" = Table.AddColumn(#"Added Custom6", "Show Org", each if ([Current Year] >= [File Year] and [File Year] <> "2024" ) then 1 else 0),
    #"Filtered Rows1" = Table.SelectRows(#"Added Custom7", each ([Show Org] = 1)),
    #"Removed Other Columns2" = Table.SelectColumns(#"Filtered Rows1",{"Extracted Data", "Service Month"}),
    #"Expanded Extracted Data1" = Table.ExpandTableColumn(#"Removed Other Columns2", "Extracted Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18"}, {"Extracted Data.Column1", "Extracted Data.Column2", "Extracted Data.Column3", "Extracted Data.Column4", "Extracted Data.Column5", "Extracted Data.Column6", "Extracted Data.Column7", "Extracted Data.Column8", "Extracted Data.Column9", "Extracted Data.Column10", "Extracted Data.Column11", "Extracted Data.Column12", "Extracted Data.Column13", "Extracted Data.Column14", "Extracted Data.Column15", "Extracted Data.Column16", "Extracted Data.Column17", "Extracted Data.Column18"}),
    #"Removed Errors" = Table.RemoveRowsWithErrors(#"Expanded Extracted Data1", {"Extracted Data.Column1"}),
    #"Added Custom8" = Table.AddColumn(#"Removed Errors", "Service Month New", each if [Extracted Data.Column1] = "Entity" then "Service Month" else [Service Month]),
    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom8",{"Service Month"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed Columns1", [PromoteAllScalars=true]),
    #"Added Custom10" = Table.AddColumn(#"Promoted Headers", "Current Month", each Date.ToText(DateTime.Date( DateTime.LocalNow()),"yyyy","en-US") & Date.ToText(DateTime.Date( DateTime.LocalNow()),"MM","en-US")),
    #"Added Custom11" = Table.AddColumn(#"Added Custom10", "Check", each if [Current Month] = [Service Month] then 0 else 1),
    #"Filtered Rows4" = Table.SelectRows(#"Added Custom11", each [Check] = 1),
    #"Added Custom12" = Table.AddColumn(#"Filtered Rows4", "Final Service Month", each [Current Month]),
    #"Removed Columns2" = Table.RemoveColumns(#"Added Custom12",{"Service Month"}),
    #"Renamed Columns4" = Table.RenameColumns(#"Removed Columns2",{{"Final Service Month", "Service Month"}}),
    #"Added Custom9" = Table.AddColumn(#"Renamed Columns4", "Org Unit ID Composite", each [Service Month] & "-" & [#"Org Unit ID (Department ID)"]),
    #"Added Custom13" = Table.AddColumn(#"Added Custom9", "Parent Org Unit ID Composite", each [Service Month] & "-" & [Parent Org Unit ID])
in
    #"Added Custom13";
shared _map_organizational_unit_monthly = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/DE1214-7332185/", [ApiVersion = 15]),
    #"Filtered Rows3" = Table.SelectRows(Source, each ([Folder Path] <> null)),
    #"Added Custom11" = Table.AddColumn(#"Filtered Rows3", "FileDate", each Text.Middle([Name],0,8)),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom11",{{"FileDate", type date}}),
    #"Inserted End of Month" = Table.AddColumn(#"Changed Type3", "End of Month", each Date.EndOfMonth([FileDate]), type datetime),
    #"Inserted Date" = Table.AddColumn(#"Inserted End of Month", "Date", each DateTime.Date([End of Month]), type date),
    #"Added Custom3" = Table.AddColumn(#"Inserted Date", "Custom", each DateTime.Date( DateTime.LocalNow())),
    #"Added Custom2" = Table.AddColumn(#"Added Custom3", "Custom.1", each DateTime.Date( DateTime.LocalNow())),
    #"Renamed Columns" = Table.RenameColumns(#"Added Custom2",{{"Date", "Date of Organization"}, {"Custom", "Date of HR"}}),
    #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns",{"End of Month"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns",{{"Date of HR", type date}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Changed Type",{{"Date of HR", "Date of HR End"}, {"Custom.1", "Date of HR Start"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns1",{{"Date of HR End", type date}}),
    #"Calculated Start of Month" = Table.TransformColumns(#"Changed Type1",{{"Date of HR Start", Date.StartOfMonth, type date}}),
    #"Added Custom4" = Table.AddColumn(#"Calculated Start of Month", "Check", each if [Date of Organization] < [Date of HR End] then 1 else 0),
    #"Filtered Rows2" = Table.SelectRows(#"Added Custom4", each ([Check] = 1)),
    #"Added Custom1" = Table.AddColumn(#"Filtered Rows2", "Extracted Data", each 
        let
            ExcelData = Excel.Workbook([Content], null, true),
            FirstSheet = ExcelData{0}[Data] // Access the first sheet by index (0-based index)
        in
            FirstSheet
    ),
    #"Added Custom" = Table.AddColumn(#"Added Custom1", "Service Month", each Date.ToText([Date of Organization],"yyyy","en-US") &Date.ToText([Date of Organization],"MM","en-US")),
    #"Filtered Rows" = Table.SelectRows(#"Added Custom", each not Text.Contains([Name], "Test")),
    #"Sorted Rows" = Table.Sort(#"Filtered Rows",{{"Date created", Order.Ascending}}),
    #"Removed Duplicates" = Table.Distinct(#"Sorted Rows", {"Service Month"}),
    #"Added Custom5" = Table.AddColumn(#"Removed Duplicates", "Custom", each Date.ToText(DateTime.Date( DateTime.LocalNow()),"yyyy","en-US")),
    #"Renamed Columns2" = Table.RenameColumns(#"Added Custom5",{{"Custom", "Current Year"}}),
    #"Added Custom6" = Table.AddColumn(#"Renamed Columns2", "File Year", each Date.ToText([Date of Organization],"yyyy","en-US")),
    #"Added Custom7" = Table.AddColumn(#"Added Custom6", "Show Org", each if ([Current Year] >= [File Year] and [File Year] <> "2024" ) then 1 else 0),
    #"Filtered Rows1" = Table.SelectRows(#"Added Custom7", each ([Show Org] = 1)),
    #"Removed Other Columns2" = Table.SelectColumns(#"Filtered Rows1",{"Extracted Data", "Service Month"}),
    #"Expanded Extracted Data" = Table.ExpandTableColumn(#"Removed Other Columns2", "Extracted Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28"}, {"Extracted Data.Column1", "Extracted Data.Column2", "Extracted Data.Column3", "Extracted Data.Column4", "Extracted Data.Column5", "Extracted Data.Column6", "Extracted Data.Column7", "Extracted Data.Column8", "Extracted Data.Column9", "Extracted Data.Column10", "Extracted Data.Column11", "Extracted Data.Column12", "Extracted Data.Column13", "Extracted Data.Column14", "Extracted Data.Column15", "Extracted Data.Column16", "Extracted Data.Column17", "Extracted Data.Column18", "Extracted Data.Column19", "Extracted Data.Column20", "Extracted Data.Column21", "Extracted Data.Column22", "Extracted Data.Column23", "Extracted Data.Column24", "Extracted Data.Column25", "Extracted Data.Column26", "Extracted Data.Column27", "Extracted Data.Column28"}),
    #"Removed Errors" = Table.RemoveRowsWithErrors(#"Expanded Extracted Data", {"Extracted Data.Column1"}),
    #"Added Custom8" = Table.AddColumn(#"Removed Errors", "Service Month New", each if [Extracted Data.Column1] = "Entity" then "Service Month" else [Service Month]),
    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom8",{"Service Month"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed Columns1", [PromoteAllScalars=true]),
    #"Added Custom9" = Table.AddColumn(#"Promoted Headers", "Org Unit ID Composite", each [Service Month] & "-" & [#"Org Unit ID (Department ID)"]),
    #"Filtered Rows5" = Table.SelectRows(#"Added Custom9", each [#"Org Unit ID (Department ID)"] <> "Org Unit ID (Department ID)"),
    #"Changed Type2" = Table.TransformColumnTypes(#"Filtered Rows5",{{"Org Unit ID Composite", type text}}),
    #"Added Custom10" = Table.AddColumn(#"Changed Type2", "Parent Org Unit ID Composite", each [Service Month] & "-" & [Parent Org Unit ID]),
    #"Removed Duplicates1" = Table.Distinct(#"Added Custom10", {"Org Unit ID Composite"}),
    #"Filtered Rows4" = Table.SelectRows(#"Removed Duplicates1", each 
    not (
        [Entity] = "Allianz Technology GmbH" and 
        [In scope for disciplinary view] = "X" and 
        ([Service Month] = "202501" or 
         [Service Month] = "202502" or 
         [Service Month] = "202503")
    )
)
in
    #"Filtered Rows4";
shared _map_organizational_unit_monthly_at_april = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/DE1214-7332185/", [ApiVersion = 15]),
    #"Filtered Rows3" = Table.SelectRows(Source, each ([Folder Path] <> null)),
    #"Added Custom11" = Table.AddColumn(#"Filtered Rows3", "FileDate", each Text.Middle([Name],0,8)),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom11",{{"FileDate", type date}}),
    #"Inserted End of Month" = Table.AddColumn(#"Changed Type3", "End of Month", each Date.EndOfMonth([FileDate]), type datetime),
    #"Inserted Date" = Table.AddColumn(#"Inserted End of Month", "Date", each DateTime.Date([End of Month]), type date),
    #"Added Custom3" = Table.AddColumn(#"Inserted Date", "Custom", each DateTime.Date( DateTime.LocalNow())),
    #"Added Custom2" = Table.AddColumn(#"Added Custom3", "Custom.1", each DateTime.Date( DateTime.LocalNow())),
    #"Renamed Columns" = Table.RenameColumns(#"Added Custom2",{{"Date", "Date of Organization"}, {"Custom", "Date of HR"}}),
    #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns",{"End of Month"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns",{{"Date of HR", type date}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Changed Type",{{"Date of HR", "Date of HR End"}, {"Custom.1", "Date of HR Start"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns1",{{"Date of HR End", type date}}),
    #"Calculated Start of Month" = Table.TransformColumns(#"Changed Type1",{{"Date of HR Start", Date.StartOfMonth, type date}}),
    #"Added Custom4" = Table.AddColumn(#"Calculated Start of Month", "Check", each if [Date of Organization] <= [Date of HR End] then 1 else 0),
    #"Filtered Rows2" = Table.SelectRows(#"Added Custom4", each ([Check] = 1)),
    #"Added Custom1" = Table.AddColumn(#"Filtered Rows2", "Extracted Data", each 
        let
            ExcelData = Excel.Workbook([Content], null, true),
            FirstSheet = ExcelData{0}[Data] // Access the first sheet by index (0-based index)
        in
            FirstSheet
    ),
    #"Added Custom" = Table.AddColumn(#"Added Custom1", "Service Month", each Date.ToText([Date of Organization],"yyyy","en-US") &Date.ToText([Date of Organization],"MM","en-US")),
    #"Filtered Rows" = Table.SelectRows(#"Added Custom", each not Text.Contains([Name], "Test")),
    #"Sorted Rows" = Table.Sort(#"Filtered Rows",{{"Date created", Order.Ascending}}),
    #"Removed Duplicates" = Table.Distinct(#"Sorted Rows", {"Service Month"}),
    #"Added Custom5" = Table.AddColumn(#"Removed Duplicates", "Custom", each Date.ToText(DateTime.Date( DateTime.LocalNow()),"yyyy","en-US")),
    #"Renamed Columns2" = Table.RenameColumns(#"Added Custom5",{{"Custom", "Current Year"}}),
    #"Added Custom6" = Table.AddColumn(#"Renamed Columns2", "File Year", each Date.ToText([Date of Organization],"yyyy","en-US")),
    #"Added Custom7" = Table.AddColumn(#"Added Custom6", "Show Org", each if ([Current Year] >= [File Year] and [File Year] <> "2024" ) then 1 else 0),
    #"Filtered Rows1" = Table.SelectRows(#"Added Custom7", each ([Show Org] = 1)),
    #"Removed Other Columns2" = Table.SelectColumns(#"Filtered Rows1",{"Extracted Data", "Service Month"}),
    #"Expanded Extracted Data" = Table.ExpandTableColumn(#"Removed Other Columns2", "Extracted Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28"}, {"Extracted Data.Column1", "Extracted Data.Column2", "Extracted Data.Column3", "Extracted Data.Column4", "Extracted Data.Column5", "Extracted Data.Column6", "Extracted Data.Column7", "Extracted Data.Column8", "Extracted Data.Column9", "Extracted Data.Column10", "Extracted Data.Column11", "Extracted Data.Column12", "Extracted Data.Column13", "Extracted Data.Column14", "Extracted Data.Column15", "Extracted Data.Column16", "Extracted Data.Column17", "Extracted Data.Column18", "Extracted Data.Column19", "Extracted Data.Column20", "Extracted Data.Column21", "Extracted Data.Column22", "Extracted Data.Column23", "Extracted Data.Column24", "Extracted Data.Column25", "Extracted Data.Column26", "Extracted Data.Column27", "Extracted Data.Column28"}),
    #"Removed Errors" = Table.RemoveRowsWithErrors(#"Expanded Extracted Data", {"Extracted Data.Column1"}),
    #"Added Custom8" = Table.AddColumn(#"Removed Errors", "Service Month New", each if [Extracted Data.Column1] = "Entity" then "Service Month" else [Service Month]),
    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom8",{"Service Month"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed Columns1", [PromoteAllScalars=true]),
    #"Added Custom9" = Table.AddColumn(#"Promoted Headers", "Org Unit ID Composite", each [Service Month] & "-" & [#"Org Unit ID (Department ID)"]),
    #"Filtered Rows5" = Table.SelectRows(#"Added Custom9", each [#"Org Unit ID (Department ID)"] <> "Org Unit ID (Department ID)"),
    #"Changed Type2" = Table.TransformColumnTypes(#"Filtered Rows5",{{"Org Unit ID Composite", type text}}),
    #"Added Custom10" = Table.AddColumn(#"Changed Type2", "Parent Org Unit ID Composite", each [Service Month] & "-" & [Parent Org Unit ID]),
    #"Removed Duplicates1" = Table.Distinct(#"Added Custom10", {"Org Unit ID Composite"}),
    #"Filtered Rows4" = Table.SelectRows(#"Removed Duplicates1", each ([Entity] = "Allianz Technology GmbH") and ([In scope for disciplinary view] = "X") and ([Service Month] = "202504"))
in
    #"Filtered Rows4";
shared _map_organizational_unit_monthly_at_mar = let
    Source = _map_organizational_unit_monthly_at_april,
    #"Replaced Value" = Table.ReplaceValue(Source,"202504","202503",Replacer.ReplaceText,{"Entity", "Service Month", "Org Unit ID Composite", "Parent Org Unit ID Composite"})
in
    #"Replaced Value";
shared _map_organizational_unit_monthly_at_feb = let
    Source = _map_organizational_unit_monthly_at_april,
    #"Replaced Value" = Table.ReplaceValue(Source,"202504","202502",Replacer.ReplaceText,{"Service Month", "Org Unit ID Composite", "Parent Org Unit ID Composite"})
in
    #"Replaced Value";
shared _map_organizational_unit_monthly_at_jan = let
    Source = _map_organizational_unit_monthly_at_april,
    #"Replaced Value" = Table.ReplaceValue(Source,"202504","202501",Replacer.ReplaceText,{"Service Month", "Org Unit ID Composite", "Parent Org Unit ID Composite"})
in
    #"Replaced Value";
shared _map_organizational_unit_parent_check = let
    Source = Table.Combine({_map_organizational_unit_last_month, _map_organizational_unit_monthly, _map_organizational_unit_monthly_at_mar, _map_organizational_unit_monthly_at_feb, _map_organizational_unit_monthly_at_jan}),
    #"Removed Columns" = Table.RemoveColumns(Source,{"Service Month", "Current Month", "Check", "Functional Child Departments - simulated"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Removed Columns",{"Org Unit ID Composite"}),
    #"Removed Duplicates" = Table.Distinct(#"Removed Other Columns")
in
    #"Removed Duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_organizational_unit_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_organizational_unit = let
    Source = Table.Combine({_map_organizational_unit_last_month, _map_organizational_unit_monthly, _map_organizational_unit_monthly_at_mar, _map_organizational_unit_monthly_at_feb, _map_organizational_unit_monthly_at_jan}),
    #"Removed Columns" = Table.RemoveColumns(Source,{"Current Month", "Check", "Functional Child Departments - simulated"}),
    #"Merged Queries" = Table.NestedJoin(#"Removed Columns", {"Parent Org Unit ID Composite"}, _map_organizational_unit_parent_check, {"Org Unit ID Composite"}, "Organizational Unit List Parent Check", JoinKind.LeftOuter),
    #"Expanded Organizational Unit List Parent Check" = Table.ExpandTableColumn(#"Merged Queries", "Organizational Unit List Parent Check", {"Org Unit ID Composite"}, {"Organizational Unit List Parent Check.Org Unit ID Composite"}),
    #"Added Custom" = Table.AddColumn(#"Expanded Organizational Unit List Parent Check", "ShowRow", each if [Parent Org Unit ID Composite] <> null and [Organizational Unit List Parent Check.Org Unit ID Composite] = null then 0 else 1),
    #"Filtered Rows" = Table.SelectRows(#"Added Custom", each ([ShowRow] = 1)),
    #"Removed Other Columns" = Table.SelectColumns(#"Filtered Rows",{"Entity", "Name of Org.Unit (dep. long name)", "Org Unit ID (Department ID)", "Name of Manager (last name)", "Name of Manager (first name)", "Manager Employee ID", "Manager email", "Parent Org Unit ID", "Cost Center", "L-1/L-2", "L-1/L-3", "L-1/L-4", "OrgUnit type", "In scope for Functional view", "In scope for disciplinary view", "Unique_ID", "Comment", "Functional Child Departments - original", "Service Month", "Org Unit ID Composite", "Parent Org Unit ID Composite"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed Other Columns", {{"Name of Org.Unit (dep. long name)", "Name Of Organizational Unit"}, {"Org Unit ID (Department ID)", "Org Unit ID"}, {"Name of Manager (last name)", "Last Name Of Manager"}, {"Name of Manager (first name)", "First Name Of Manager"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"L-1/L-2", "L-1/L-3", "L-1/L-4", "In scope for Functional view", "In scope for disciplinary view", "Comment", "Functional Child Departments - original"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Entity", type text}, {"Name Of Organizational Unit", type text}, {"Org Unit ID", type text}, {"Last Name Of Manager", type text}, {"First Name Of Manager", type text}, {"Manager Employee ID", type text}, {"Manager email", type text}, {"Parent Org Unit ID", type text}, {"Cost Center", type text}, {"OrgUnit type", type text}, {"Unique_ID", type text}, {"Service Month", type text}, {"Org Unit ID Composite", type text}, {"Parent Org Unit ID Composite", type text}})
in
    #"Changed column type";
shared dim_organizational_unit_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_organizational_unit", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_rate_charged_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_rate_charged = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Rate Charged", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Resource Type", type text}, {"Addon1", type text}, {"Addon2", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_exchange_rates_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_exchange_rates = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Exchange Rates", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Currency", type text}, {"EUR", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_planning_versions_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_planning_versions = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Planning Versions", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Version", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_billing_run_calendar_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_billing_run_calendar = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Billing Run Calendar", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Month Date", type text}, {"Date To ICC", type text}, {"Date From ICC", type text}, {"Date To", type text}, {"Date From", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_grade_proficiency_level_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_grade_proficiency_level = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Grade and Proficiency Levels", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Grade", type text}, {"Proficiency Level", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_hr_capacity_parameters_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_hr_capacity_parameters = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "HR Capacity Parameters", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Sales Organisation", type text}, {"Annual Capacity in PDs", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_user_rights_country_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_user_rights_country = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "User Rights Per Country", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"User", type text}, {"Flag", type text}, {"Role", type text}, {"Location", type text}, {"Employee lists", type text}, {"PPM Time Bookings", type text}, {"Cost & Revenue", type text}, {"Service Spend", type text}, {"Budget Availability", type text}, {"Account Receivables", type text}, {"Column11", type text}, {"Column12", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Column11", "Column12"})
in
    #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_cost_centers_rate_cards_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_cost_centers_rate_cards = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Cost Centers", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Cost Center", type text}, {"Invoicing Method", type text}, {"Rate Card", type text}})
in
    #"Changed column type";
shared map_rate_charged_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_rate_charged", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_exchange_rates_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_exchange_rates", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_planning_versions_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_planning_versions", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_billing_run_calendar_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_billing_run_calendar", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_grade_proficiency_level_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_grade_proficiency_level", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_hr_capacity_parameters_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_hr_capacity_parameters", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_user_rights_country_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_user_rights_country", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_cost_centers_rate_cards_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_cost_centers_rate_cards", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_rate_card_sap_translation_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_rate_card_sap_translation = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Rate Cards SAP Translation", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Removed columns" = Table.RemoveColumns(#"Promoted headers", {"Column7", "Column6"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"Material", type text}, {"Material Name", type text}, {"Material Name Corrected", type text}, {"Material Name PPM", type text}, {"Rate Role", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_at45_grades_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_at45_grades = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "AT45 Grades", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Removed columns 1" = Table.RemoveColumns(#"Promoted headers", {"Position Cluster (Picklist Label)", "Führungskraft (Name)", "Mitarbeiterstatus (Picklist Label)"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Lokale Personalnummer", type text}, {"Mitarbeiterkennung", type text}, {"Vorname", type text}, {"Nachname", type text}, {"Arbeitgebergesellschaft (Label)", type text}, {"Grades", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_at45_employees_master_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_at45_employees_master_data = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "AT45 Employee Master Data", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Personal Number", type text}, {"First Name", type text}, {"Last Name", type text}, {"Daily Rate", type text}, {"Jobrole NEU", type text}, {"Contract Type", type text}, {"Date of Birth", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_de44_employee_master_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_de44_employee_master_data = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "DE44 Employee Master Data", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Personal Number", type text}, {"First Name", type text}, {"Last Name", type text}, {"Email", type text}, {"Year", type text}, {"Grade", type text}, {"Gender", type text}, {"User ID", type text}, {"Role Type", type text}, {"Functional OrgUnit ID", type text}, {"Disciplinary OrgUnit ID", type text}, {"Position", type text}, {"Contract Type", type text}, {"My HR Manager", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_cost_element_fc_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_cost_element_fc = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
    Company_Table = Source{[Item="FCCostElementMapping",Kind="Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(Company_Table, {{"Forecast Cluster /SAC Mask", "Forecast Cluster"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Cost Element", type text}, {"Cost Element Name", type text}, {"Forecast Cost Element Hierarchy", type text}, {"Forecast Default Cost Element", type text}, {"Forecast Default Cost Element Name", type text}, {"Forecast Cluster", type text}})
in
    #"Changed column type";
shared map_de44_employee_master_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_de44_employee_master_data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_at45_employees_master_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_at45_employees_master_data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_at45_grades_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_at45_grades", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_rate_card_sap_translation_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_rate_card_sap_translation", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_cost_element_fc_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cost_element_fc", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_asset_class_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_asset_class = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
    AssetClass_Table = Source{[Item="AssetClass",Kind="Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(AssetClass_Table, {"Class Asset", "Class Name", "Capex Hierarchy"}),
  #"Added custom" = Table.AddColumn(#"Removed other columns", "Asset Class and Name", each [Class Asset] & " - " & [Class Name]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Class Asset", type text}, {"Asset Class and Name", type text}, {"Capex Hierarchy", type text}, {"Class Name", type text}})
in
    #"Changed column type";
shared dim_asset_class_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_asset_class", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_service_mapping_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_service_mapping = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
    ServiceMapping_Table = Source{[Item="ServiceMapping",Kind="Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(ServiceMapping_Table, {{"Material", type text}, {"Internal IT Cost", type text}, {"Category", type text}})
in
    #"Changed column type";
shared map_service_mapping_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_service_mapping", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_document_type_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_document_type = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Document Type", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Removed columns" = Table.RemoveColumns(#"Promoted headers", {"Column1", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"Code", type text}, {"English Name", type text}, {"German Name", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_gl_accounts_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_gl_accounts = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "GL Accounts", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"GL Account", type text}, {"GL Account Name", type text}})
in
    #"Changed column type";
shared map_document_type_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_document_type", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared map_gl_accounts_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_gl_accounts", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_tax_rates_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_tax_rates = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
  Navigation = Source{[Item = "Tax Rates", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Sales Organisation", type text}, {"Tax Rate", type text}})
in
    #"Changed column type";
shared map_tax_rates_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_tax_rates", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

