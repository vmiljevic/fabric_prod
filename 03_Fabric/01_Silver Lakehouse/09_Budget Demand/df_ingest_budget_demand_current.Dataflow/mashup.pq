[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_budget_demand_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}, PreviewOnlySteps = {"Kept top rows"}]
shared fact_budget_demand_current = let
    // SPEED TIP: Read only metadata first
    SP = SharePoint.Files(
            "https://allianzms.sharepoint.com/teams/AT0090-10144432/",
            [ApiVersion = 15]
        ),

    // Filter early
    Filtered =
        Table.SelectRows(
            SP,
            each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP BW/Budget Demand/")
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
  CurrentAndFutureYear = Table.SelectRows(AddYear, each [Folder Path] >= CurrentYear),

    // Keep only required columns
    KeepCols =
        Table.SelectColumns(
            CurrentAndFutureYear,
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
  #"Removed top rows" = Table.Skip(Combined, 2),

    // Add consistent File / Refresh columns
    AddMeta =
        Table.AddColumn(
            Table.AddColumn(#"Removed top rows", "File Name", each [Name]),
            "Refresh Date",
            each [Date modified]
        ),

    Removed =
        Table.RemoveColumns(AddMeta, { "Content", "Name", "Date modified" }),
  #"Filtered rows" = Table.SelectRows(Removed, each ([Company Code] <> "" or [Company Code] <> "Company Code")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each ([Profit Center] <> null and [Profit Center] <> "" and [Profit Center] <> "#" and [Profit Center] <> "Profit center")),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows 1", {"SAP Refresh Time"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"Company Code", type text}, {"Pillar", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Order Type", type text}, {"Order Type Name", type text}, {"Order", type text}, {"Demand ID", type text}, {"Purchasing order number (PO)", type text}, {"Plan ID (MM PO)", type text}, {"CO Doc Line Item Txt", type text}, {"Vendor", type text}, {"Vendor Name", type text}, {"WBS (Acc.)", type text}, {"WBS Name", type text}, {"Fiscal year", type text}, {"Budget #(lf)CO Order", type text}, {"Actual #(lf)CO Order", type text}, {"Commitment#(lf)Total", type text}, {"Commitment #(lf)P.Requisition", type text}, {"Commitment #(lf)P.Order", type text}, {"Actual #(lf)Alloted", type text}, {"Budget #(lf)Available", type text}, {"Demandbook#(lf)Cash Out Total #(lf)inc. VAT", type text}, {"Demandbook#(lf)Cash Out BudgetYear #(lf)inc. VAT", type text}, {"Demandbook #(lf)Cost effective BudgetYear #(lf)inc. VAT", type text}, {"Cash Out #(lf)Planning Year #(lf)(Net)", type text}, {"Cash Out #(lf)Plan Year #(lf)(Gross)", type text}, {"Net purchase order value (PR) - All", type text}, {"Net purchase order value (PR) - New without PO", type text}, {"Net purchase order value (PO)", type text}, {"Net purchase order value (PO+PR new)", type text}, {"Budget remaining (Cash out Total)", type text}, {"File Name", type text}, {"Refresh Date", type text}})
in
    #"Changed column type";
shared fact_budget_demand_current_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_budget_demand_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
