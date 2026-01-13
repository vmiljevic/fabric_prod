[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared at45_nonproductive_itat = let
    // 1️⃣ Connect to SharePoint site
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each ([Extension] = ".csv") and ([Name] = "AT45_Overhead IT AT Internal.csv")),

    // 3️⃣ Remove hidden/system files
    VisibleOnly = Table.SelectRows(#"Filtered Rows", each [Attributes]?[Hidden]? <> true),

    // 4️⃣ Get the binary content of the CSV file
    FileBinary = VisibleOnly{0}[Content],

    // 5️⃣ Read CSV content directly
    CsvImported = Csv.Document(
        FileBinary,
        [
            Delimiter = ",",
            Encoding = 65001,
            QuoteStyle = QuoteStyle.None
        ]
    ),

    // 6️⃣ Promote headers and optionally change types
    #"Promoted Headers" = Table.PromoteHeaders(CsvImported, [PromoteAllScalars=true]),
    #"Removed Columns" = Table.RemoveColumns(#"Promoted Headers",{"_2"}),
    #"Unpivoted Other Columns" = Table.UnpivotOtherColumns(#"Removed Columns", {"", "_1"}, "Attribute", "Value"),
    #"Renamed Columns" = Table.RenameColumns(#"Unpivoted Other Columns",{{"", "employee email"}, {"_1", "activity"}, {"Attribute", "Month"}, {"Value", "Hours"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns",".",",",Replacer.ReplaceText,{"Hours"}),
    #"Filtered Rows2" = Table.SelectRows(#"Replaced Value", each ([Hours] <> "")),
    #"Split Column by Delimiter" = Table.SplitColumn(#"Filtered Rows2", "activity", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv, false), {"activity.1", "activity.2"}),
    #"Removed Columns1" = Table.RemoveColumns(#"Split Column by Delimiter",{"activity.1"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns1",{{"activity.2", "Activity"}}),
    #"Replaced Value1" = Table.ReplaceValue(#"Renamed Columns1","Chapter PO","Line activities",Replacer.ReplaceText,{"Activity"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","[Service Steering] Learning","Formal Trainings",Replacer.ReplaceText,{"Activity"}),
    #"Filtered Rows1" = Table.SelectRows(#"Replaced Value2", each ([Month] <> "Feb 2025" and [Month] <> "Jan 2025")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered Rows1", {{"employee email", type text}, {"Activity", type text}, {"Month", type text}, {"Hours", type text}})
in
    #"Changed column type";
shared at45_nonproductive_core = let
        // 1️⃣ Connect to SharePoint site
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each ([Extension] = ".csv") and ([Name] = "AT45_Overhead ABS Core Tracking.csv")),

    // 3️⃣ Remove hidden/system files
    VisibleOnly = Table.SelectRows(#"Filtered Rows", each [Attributes]?[Hidden]? <> true),

    // 4️⃣ Get the binary content of the CSV file
    FileBinary = VisibleOnly{0}[Content],

    // 5️⃣ Read CSV content directly
    CsvImported = Csv.Document(
        FileBinary,
        [
            Delimiter = ",",
            Encoding = 65001,
            QuoteStyle = QuoteStyle.None
        ]
    ),
    #"Removed Top Rows" = Table.Skip(CsvImported,1),

    // 6️⃣ Promote headers and optionally change types

    #"Promoted Headers" = Table.PromoteHeaders(#"Removed Top Rows", [PromoteAllScalars=true]),
    #"Unpivoted Other Columns" = Table.UnpivotOtherColumns(#"Promoted Headers", {"", "_1"}, "Attribute", "Value"),
    #"Renamed Columns" = Table.RenameColumns(#"Unpivoted Other Columns",{{"", "employee email"}, {"_1", "activity"}, {"Attribute", "Month"}, {"Value", "Hours"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns",".",",",Replacer.ReplaceText,{"Hours"}),
    #"Filtered Rows2" = Table.SelectRows(#"Replaced Value", each ([Hours] <> "")),
    #"Split Column by Delimiter" = Table.SplitColumn(#"Filtered Rows2", "activity", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv, false), {"activity.1", "activity.2"}),
    #"Removed Columns1" = Table.RemoveColumns(#"Split Column by Delimiter",{"activity.1"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns1",{{"activity.2", "Activity"}}),
    #"Filtered Rows1" = Table.SelectRows(#"Renamed Columns1", each ([Activity] <> "Absences")),
    #"Replaced Value1" = Table.ReplaceValue(#"Filtered Rows1","Branch Events-AzTech GmbH","Corporate Events",Replacer.ReplaceText,{"Activity"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","Management Support Activities","Line activities",Replacer.ReplaceText,{"Activity"}),
    #"Replaced Value3" = Table.ReplaceValue(#"Replaced Value2","Unplanned Core Support (Specific User)","Line activities",Replacer.ReplaceText,{"Activity"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Replaced Value3", {{"Month", type text}, {"Hours", type text}, {"employee email", type text}, {"Activity", type text}})
in
    #"Changed Type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "at45_noproductive_jira_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_at45_jmp_nonproductive = let
  Source = Table.Combine({at45_nonproductive_core, at45_nonproductive_itat})
in
  Source;
shared at45_noproductive_jira_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_at45_jmp_nonproductive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
