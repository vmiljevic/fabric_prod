[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_o2i_pre_run_cost_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}, PreviewOnlySteps = {"Kept top rows"}]
shared fact_o2i_pre_run_cost_archive = let
    // SPEED TIP: Read only metadata first
    SP = SharePoint.Files(
            "https://allianzms.sharepoint.com/teams/AT0090-10144432/",
            [ApiVersion = 15]
        ),

    // Filter early
    Filtered =
        Table.SelectRows(
            SP,
            each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP BW/O2I/Costs")
        ),

    // Buffer list of files → avoids re-scanning SharePoint
    Files = Table.Buffer(Filtered),

    // Extract year from folder once
    AddYear =
        Table.TransformColumns(
            Files,
            { { "Folder Path", each Text.Start(Text.End(_, 5), 4), type text } }
        ),

    CurrentYear = Text.From(Date.Year(DateTime.LocalNow())),
    OnlyPreviousYears = Table.SelectRows(AddYear, each [Folder Path] < CurrentYear),

    // Keep only required columns
    KeepCols =
        Table.SelectColumns(
            OnlyPreviousYears,
            { "Content", "Name", "Date modified" }
        ),

    // Function to read each Excel file quickly
    ReadExcel = (binary as binary) =>
        let
            WB = Excel.Workbook(binary, null, true),
            Sheet = WB{[Item = "Sheet1", Kind = "Sheet"]}[Data],
            Promote = Table.PromoteHeaders(Sheet, [PromoteAllScalars = true])
        in
            Promote,

    // Expand by reading each file only once
    Expanded =
        Table.AddColumn(
            KeepCols,
            "Data",
            each ReadExcel([Content]),
            type table
        ),

    Combined =
        Table.ExpandTableColumn(
            Expanded,
            "Data",
            Table.ColumnNames(Expanded[Data]{0})
        ),

    // Add consistent File / Refresh columns
    AddMeta =
        Table.AddColumn(
            Table.AddColumn(Combined, "File Name", each [Name]),
            "Refresh Date",
            each [Date modified]
        ),

    Removed =
        Table.RemoveColumns(AddMeta, { "Content", "Name", "Date modified" }),
  #"Changed column type" = Table.TransformColumnTypes(Removed, {{"SAP Refresh Time", type text}, {"Account Assigned To", type text}, {"Booking text SNow", type text}, {"Cal. Year/Month of Load", type text}, {"Charging Type", type text}, {"Consumption Key", type text}, {"Cost Center", type text}, {"Cost Center Orig. (Service Now)", type text}, {"Customer Tag", type text}, {"Debitor Original (Service Now)", type text}, {"Feeder Item ID", type text}, {"Feeder System", type text}, {"Feeder User Name", type text}, {"GIAM ID Assigned To", type text}, {"GIAM ID Owner", type text}, {"Load Date", type text}, {"Material", type text}, {"Owner Account", type text}, {"Payer", type text}, {"Primary Name", type text}, {"Product Name", type text}, {"Product Order ID", type text}, {"Quote ID", type text}, {"Sales Organization", type text}, {"Secondary Name", type text}, {"Service Date", type text}, {"Service Instance ID", type text}, {"SI Location", type text}, {"SI Name", type text}, {"SI Provider", type text}, {"SI SLA", type text}, {"SI Status", type text}, {"Timestamp Load", type text}, {"WBS Elem. Original (Service Now)", type text}, {"WBS Element", type text}, {"Quantity", type text}, {"File Name", type text}, {"Refresh Date", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_o2i_pre_run_revenue_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}, PreviewOnlySteps = {"Kept top rows"}]
shared fact_o2i_pre_run_revenue_archive = let
    // SPEED TIP: Read only metadata first
    SP = SharePoint.Files(
            "https://allianzms.sharepoint.com/teams/AT0090-10144432/",
            [ApiVersion = 15]
        ),

    // Filter early
    Filtered =
        Table.SelectRows(
            SP,
            each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP BW/O2I/Revenue")
        ),

    // Buffer list of files → avoids re-scanning SharePoint
    Files = Table.Buffer(Filtered),

    // Extract year from folder once
    AddYear =
        Table.TransformColumns(
            Files,
            { { "Folder Path", each Text.Start(Text.End(_, 5), 4), type text } }
        ),

    CurrentYear = Text.From(Date.Year(DateTime.LocalNow())),
    OnlyPreviousYears = Table.SelectRows(AddYear, each [Folder Path] < CurrentYear),

    // Keep only required columns
    KeepCols =
        Table.SelectColumns(
            OnlyPreviousYears,
            { "Content", "Name", "Date modified" }
        ),

    // Function to read each Excel file quickly
    ReadExcel = (binary as binary) =>
        let
            WB = Excel.Workbook(binary, null, true),
            Sheet = WB{[Item = "Sheet1", Kind = "Sheet"]}[Data],
            Promote = Table.PromoteHeaders(Sheet, [PromoteAllScalars = true])
        in
            Promote,

    // Expand by reading each file only once
    Expanded =
        Table.AddColumn(
            KeepCols,
            "Data",
            each ReadExcel([Content]),
            type table
        ),

    Combined =
        Table.ExpandTableColumn(
            Expanded,
            "Data",
            Table.ColumnNames(Expanded[Data]{0})
        ),

    // Add consistent File / Refresh columns
    AddMeta =
        Table.AddColumn(
            Table.AddColumn(Combined, "File Name", each [Name]),
            "Refresh Date",
            each [Date modified]
        ),

    Removed =
        Table.RemoveColumns(AddMeta, { "Content", "Name", "Date modified" }),
  #"Changed column type" = Table.TransformColumnTypes(Removed, {{"SAP Refresh Time", type text}, {"Account Assigned To", type text}, {"Booking text SNow", type text}, {"Cal. Year/Month of Load", type text}, {"Charging Type", type text}, {"Consumption Key", type text}, {"Cost Center", type text}, {"Cost Center Orig. (Service Now)", type text}, {"Customer Tag", type text}, {"Debitor Original (Service Now)", type text}, {"Feeder Item ID", type text}, {"Feeder System", type text}, {"Feeder User Name", type text}, {"GIAM ID Assigned To", type text}, {"GIAM ID Owner", type text}, {"Load Date", type text}, {"Material", type text}, {"Owner Account", type text}, {"Payer", type text}, {"Primary Name", type text}, {"Product Name", type text}, {"Product Order ID", type text}, {"Quote ID", type text}, {"Sales Organization", type text}, {"Secondary Name", type text}, {"Service Date", type text}, {"Service Instance ID", type text}, {"SI Location", type text}, {"SI Name", type text}, {"SI Provider", type text}, {"SI SLA", type text}, {"SI Status", type text}, {"Timestamp Load", type text}, {"WBS Elem. Original (Service Now)", type text}, {"WBS Element", type text}, {"Quantity", type text}, {"File Name", type text}, {"Refresh Date", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_o2i_status_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_o2i_status_archive = let
    // SPEED TIP: Read only metadata first
    SP = SharePoint.Files(
            "https://allianzms.sharepoint.com/teams/AT0090-10144432/",
            [ApiVersion = 15]
        ),

    // Filter early
    Filtered =
        Table.SelectRows(
            SP,
            each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP BW/O2I/Status")
        ),

    // Buffer list of files → avoids re-scanning SharePoint
    Files = Table.Buffer(Filtered),

    // Extract year from folder once
    AddYear =
        Table.TransformColumns(
            Files,
            { { "Folder Path", each Text.Start(Text.End(_, 5), 4), type text } }
        ),

    CurrentYear = Text.From(Date.Year(DateTime.LocalNow())),
  OnlyCurrentYear = Table.SelectRows(AddYear, each [Folder Path] < CurrentYear),

    // Keep only required columns
    KeepCols =
        Table.SelectColumns(
            OnlyCurrentYear,
            { "Content", "Name", "Date modified" }
        ),

    // Function to read each Excel file quickly
    ReadExcel = (binary as binary) =>
        let
            WB = Excel.Workbook(binary, null, true),
            Sheet = WB{[Item = "Sheet1", Kind = "Sheet"]}[Data],
            Promote = Table.PromoteHeaders(Sheet, [PromoteAllScalars = true])
        in
            Promote,

    // Expand by reading each file only once
    Expanded =
        Table.AddColumn(
            KeepCols,
            "Data",
            each ReadExcel([Content]),
            type table
        ),

    Combined =
        Table.ExpandTableColumn(
            Expanded,
            "Data",
            Table.ColumnNames(Expanded[Data]{0})
        ),

    // Add consistent File / Refresh columns
    AddMeta =
        Table.AddColumn(
            Table.AddColumn(Combined, "File Name", each [Name]),
            "Refresh Date",
            each [Date modified]
        ),

    Removed =
        Table.RemoveColumns(AddMeta, {"Content", "Name", "Date modified", "SAP Refresh Time"}),
  #"Changed column type" = Table.TransformColumnTypes(Removed, {{"Refresh Date", type text}, {"Service Date", type text}, {"CAP Error Message", type text}, {"Quote ID", type text}, {"Quantity Snow deliverd", type text}, {"File Name", type text}})
in
    #"Changed column type";
shared fact_o2i_status_archive_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_o2i_status_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_o2i_pre_run_cost_archive_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_o2i_pre_run_cost_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_o2i_pre_run_revenue_archive_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_o2i_pre_run_revenue_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
