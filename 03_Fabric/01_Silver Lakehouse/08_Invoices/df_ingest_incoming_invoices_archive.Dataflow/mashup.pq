[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_incoming_invoices_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}, PreviewOnlySteps = {"Kept top rows"}]
shared fact_incoming_invoices_archive = let
    // SPEED TIP: Read only metadata first
    SP = SharePoint.Files(
            "https://allianzms.sharepoint.com/teams/AT0090-10144432/",
            [ApiVersion = 15]
        ),

    // Filter early
    Filtered =
        Table.SelectRows(
            SP,
            each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP CAP/Incoming Invoices/")
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
        Table.RemoveColumns(AddMeta, { "Content", "Name", "Date modified" }),
  #"Changed column type" = Table.TransformColumnTypes(Removed, {{"Cleared/open items symbol", type text}, {"Company Code", type text}, {"Account", type text}, {"G/L Account", type text}, {"Document Number", type text}, {"Document Date", type text}, {"Clearing Document", type text}, {"Document Type", type text}, {"Posting Date", type text}, {"Fiscal Year", type text}, {"Posting Period", type text}, {"Net due date", type text}, {"Posting Key", type text}, {"Amount in doc. curr.", type text}, {"Document currency", type text}, {"Amount in local currency", type text}, {"Local Currency", type text}, {"Amount in loc.curr.2", type text}, {"Local Currency 2", type text}, {"Trading partner", type text}, {"Reference", type text}, {"Document Header Text", type text}, {"Text", type text}, {"User Name", type text}, {"File Name", type text}, {"Refresh Date", type text}})
in
    #"Changed column type";
shared fact_incoming_invoices_current_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_incoming_invoices_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
