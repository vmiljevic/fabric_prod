[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cost_element_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_cost_element_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_cost_element_current", ItemKind = "Table"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 2", "L3 Code and Name", each [L3 Code] & " - " & [L3 Name]),
  #"Merged queries" = Table.NestedJoin(#"Added custom", {"Cost Element"}, dim_cost_element_fc_current, {"Cost Element"}, "dim_cost_element_fc_current", JoinKind.LeftOuter),
  #"Expanded dim_cost_element_fc_current" = Table.ExpandTableColumn(#"Merged queries", "dim_cost_element_fc_current", {"Forecast Cost Element Hierarchy", "Forecast Cluster"}, {"Forecast Cost Element Hierarchy", "Forecast Cluster"}),
  #"Added custom 1" = Table.AddColumn(#"Expanded dim_cost_element_fc_current", "Refresh Date", each DateTime.LocalNow() + #duration(0,1,0,0)),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 1", {{"L3 Code and Name", type text}, {"Refresh Date", type datetime}})
in
  #"Changed column type";
shared dim_cost_element_fc_current = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dim_cost_element_fc", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_company_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_company = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dim_company", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared dim_cost_element_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cost_element", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sec_user_right_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared sec_user_right = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "sec_user_right", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 3", {"Flag", "Role", "Location"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"User", type text}, {"Employee lists", type text}, {"PPM Time Bookings", type text}, {"Cost & Revenue", type text}, {"Service Spend", type text}, {"Budget Availability", type text}, {"Account Receivables", type text}})
in
  #"Changed column type";
shared sec_user_right_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "sec_user_right", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_date_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_date = let
    // Define date range
    StartDate = #date(2016, 6, 1),
    EndDate   = #date(2028, 12, 31),

    // Create list of all days
    DateList = List.Dates(
        StartDate,
        Duration.Days(EndDate - StartDate) + 1,
        #duration(1,0,0,0)
    ),

    // Convert to table
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),

    // Add core parts
    AddYear = Table.AddColumn(DateTable, "Year", each Date.Year([Date]), Int64.Type),
    AddQuarterNumber = Table.AddColumn(AddYear, "Quarter Number", each Date.QuarterOfYear([Date]), Int64.Type),
    AddQuarterName = Table.AddColumn(AddQuarterNumber, "Quarter Name", each "Q" & Text.From(Date.QuarterOfYear([Date]))),
    AddMonthNumberShort = Table.AddColumn(AddQuarterName, "Month Number Short", each Date.Month([Date]), Int64.Type),
    AddMonthNameShort = Table.AddColumn(AddMonthNumberShort, "Month Name Short", each Date.ToText([Date], "MMM", "en-US")),
    AddMonthService = Table.AddColumn(AddMonthNameShort, "Month Service", each Date.ToText([Date], "MMM yyyy", "en-US")),
    AddMonthNumber = Table.AddColumn(AddMonthService, "Month Number", each Number.FromText(Date.ToText([Date], "yyMM")), Int64.Type),

    // First day of month
    AddMonthDate = Table.AddColumn(AddMonthNumber, "Month Date", each #date(Date.Year([Date]), Date.Month([Date]), 1), type date),

    // Flags
    CurrentYYMM = Number.FromText(Date.ToText(DateTime.Date(DateTime.LocalNow()), "yyMM")),
    AddIsCurrentMonth = Table.AddColumn(AddMonthDate, "Is Current Month", each CurrentYYMM = Number.FromText(Date.ToText([Date], "yyMM")), type logical),
    AddIsPreviousMonth = Table.AddColumn(AddIsCurrentMonth, "Is Previous Month", each CurrentYYMM - 1 = Number.FromText(Date.ToText([Date], "yyMM")), type logical),
    AddIsCurrentYear = Table.AddColumn(AddIsPreviousMonth, "Is Current Year", each Date.Year(DateTime.Date(DateTime.LocalNow())) = Date.Year([Date]), type logical),

    // Reorder for clarity
    Result = Table.ReorderColumns(AddIsCurrentYear,{
        "Date","Year","Quarter Number","Quarter Name","Month Number Short","Month Name Short",
        "Month Service","Month Number","Month Date","Is Current Month","Is Previous Month","Is Current Year"
    }),
  #"Changed column type" = Table.TransformColumnTypes(Result, {{"Date", type text}, {"Quarter Name", type text}, {"Month Name Short", type text}, {"Month Service", type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"Date", type date}})
in
    #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "_measures_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared _measures = let
    Source = #table(type table [Placeholder = text], { {"KeepForMeasures"} })
in
    Source;
shared dim_date_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_date", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared _measures_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "_measures", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_organizational_unit = let
    //--------------------------------------
    // Load from Lakehouse
    //--------------------------------------
    Source      = Lakehouse.Contents(null),
    Workspace   = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    Lakehouse   = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
    TblRaw      = Lakehouse{[Id="dim_organizational_unit", ItemKind="Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(TblRaw, each [Org Unit ID Composite] <> null),

    //--------------------------------------
    // Normalize key fields
    //--------------------------------------
    Tbl = Table.TransformColumns(
        #"Filtered rows",
        {
            {"Org Unit ID Composite", each if _ <> null then Text.Upper(Text.Trim(Text.From(_))) else null, type text},
            {"Parent Org Unit ID Composite", each if _ <> null then Text.Upper(Text.Trim(Text.From(_))) else null, type text},
            {"First Name Of Manager", each if _ <> null then Text.Trim(Text.From(_)) else null, type text},
            {"Last Name Of Manager",  each if _ <> null then Text.Trim(Text.From(_)) else null, type text},
            {"Manager email",         each if _ <> null then Text.Trim(Text.From(_)) else null, type text}
        }
    ),

    //--------------------------------------
    // ORG hierarchy (TOP → CURRENT): L1..L8
    //--------------------------------------
    SlimParents = Table.SelectColumns(Tbl, {"Org Unit ID Composite", "Parent Org Unit ID Composite"}),

    // Clean + deduplicate
    SlimParentsClean =
        Table.TransformColumns(
            Table.Distinct(SlimParents, {"Org Unit ID Composite"}),
            {
                {"Org Unit ID Composite", each if _<>null then Text.Upper(Text.Trim(Text.From(_))) else null, type text},
                {"Parent Org Unit ID Composite", each if _<>null then Text.Upper(Text.Trim(Text.From(_))) else null, type text}
            }
        ),

    // Fast parent lookup
    ParentLookup = Record.FromTable(
        Table.RenameColumns(
            SlimParentsClean,
            {{"Org Unit ID Composite","Name"},{"Parent Org Unit ID Composite","Value"}}
        )
    ),

    // Fixed-depth parent climb (right-padded)
    Cur0  = Table.AddColumn(SlimParentsClean, "Cur", each [Org Unit ID Composite], type text),
    P1    = Table.AddColumn(Cur0, "Parent1", each if [Cur] <> null then Record.FieldOrDefault(ParentLookup, [Cur]) else null, type text),
    P2    = Table.AddColumn(P1,   "Parent2", each if [Parent1] <> null then Record.FieldOrDefault(ParentLookup, [Parent1]) else null, type text),
    P3    = Table.AddColumn(P2,   "Parent3", each if [Parent2] <> null then Record.FieldOrDefault(ParentLookup, [Parent2]) else null, type text),
    P4    = Table.AddColumn(P3,   "Parent4", each if [Parent3] <> null then Record.FieldOrDefault(ParentLookup, [Parent3]) else null, type text),
    P5    = Table.AddColumn(P4,   "Parent5", each if [Parent4] <> null then Record.FieldOrDefault(ParentLookup, [Parent4]) else null, type text),
    P6    = Table.AddColumn(P5,   "Parent6", each if [Parent5] <> null then Record.FieldOrDefault(ParentLookup, [Parent5]) else null, type text),
    P7    = Table.AddColumn(P6,   "Parent7", each if [Parent6] <> null then Record.FieldOrDefault(ParentLookup, [Parent6]) else null, type text),

    // Build Top→Current list, right-pad with nulls
    WithList =
        Table.AddColumn(
            P7,
            "OrgHierarchy",
            each
                let chain = List.RemoveNulls({[Cur],[Parent1],[Parent2],[Parent3],[Parent4],[Parent5],[Parent6],[Parent7]}),
                    topToCurrent = List.Reverse(chain),
                    padded = if List.Count(topToCurrent) < 8
                             then List.Combine({topToCurrent, List.Repeat({null}, 8 - List.Count(topToCurrent))})
                             else List.FirstN(topToCurrent, 8)
                in padded
        ),

    // Expand Org Unit L1..L8 (nullable text)
    WithLCols =
        List.Accumulate(
            {1..8},
            WithList,
            (st,i)=> Table.AddColumn(st, "Org Unit L" & Text.From(i), each let v=[OrgHierarchy]{i-1}? in if v=null or v="" then null else v, type nullable text)
        ),

    OrgClean = Table.RemoveColumns(WithLCols, {"OrgHierarchy","Cur","Parent1","Parent2","Parent3","Parent4","Parent5","Parent6","Parent7"}),

    //--------------------------------------
    // Manager lookups per level
    //--------------------------------------
    EnrichLevel = (state as table, level as number) as table =>
        let
            lvlCol = "Org Unit L" & Text.From(level),
            joinName = "J_L" & Text.From(level),
            j = Table.NestedJoin(state, {lvlCol}, Tbl, {"Org Unit ID Composite"}, joinName, JoinKind.LeftOuter),
            e = Table.ExpandTableColumn(
                    j, joinName,
                    {"Manager email","First Name Of Manager","Last Name Of Manager"},
                    {"Manager Email L" & Text.From(level), "M" & Text.From(level) & "_FName", "M" & Text.From(level) & "_LName"}
                ),
            n = Table.AddColumn(
                    e,
                    "Manager L" & Text.From(level),
                    each
                        let f = Record.FieldOrDefault(_, "M" & Text.From(level) & "_FName"),
                            l = Record.FieldOrDefault(_, "M" & Text.From(level) & "_LName"),
                            full = Text.Trim(Text.Combine({ if f=null then "" else f, if l=null then "" else l }, " "))
                        in if full = "" then null else full,
                    type text
                ),
            d = Table.RemoveColumns(n, {"M" & Text.From(level) & "_FName","M" & Text.From(level) & "_LName"})
        in
            d,

    WithManagers = List.Accumulate({1..8}, OrgClean, (st,i)=>EnrichLevel(st, i)),

    //--------------------------------------
    // Merge back original fields
    //--------------------------------------
    TblRaw_Renamed =
        Table.RenameColumns(
            #"Filtered rows",
            {
                {"Org Unit ID Composite", "Org Unit ID Composite_Original"},
                {"Parent Org Unit ID Composite", "Parent Org Unit ID Composite_Original"}
            },
            MissingField.Ignore
        ),

    Merged = Table.Join(
        WithManagers,
        {"Org Unit ID Composite"},
        TblRaw_Renamed,
        {"Org Unit ID Composite_Original"},
        JoinKind.LeftOuter
    ),

    CleanMerged = Table.RemoveColumns(Merged, {"Org Unit ID Composite_Original", "Parent Org Unit ID Composite_Original"}),

    //--------------------------------------
    // Remove any YYYYMM- prefix from Org Unit IDs
    //--------------------------------------
    StripPrefix =
        (t as nullable text) as nullable text =>
            if t=null then null
            else
                let txt = Text.From(t)
                in
                    if Text.Length(txt) >= 8 and Text.Range(txt,6,1) = "-" and Value.Is(Value.FromText(Text.Range(txt,0,6)), type number)
                    then Text.Range(txt,7)
                    else txt,

    RemovePrefix =
        List.Accumulate(
            {1..8},
            CleanMerged,
            (st,i)=> Table.TransformColumns(st, {{"Org Unit L"&Text.From(i), StripPrefix, type nullable text}})
        ),

    //--------------------------------------
    // Path columns (safe join)
    //--------------------------------------
    NonEmpty =
        (lst as list) as list =>
            List.Select(lst, each _ <> null and Text.Trim(Text.From(_)) <> ""),

    AddOrgPath =
        Table.AddColumn(
            RemovePrefix,
            "OrgPath",
            each Text.Combine(
                NonEmpty({[#"Org Unit L1"],[#"Org Unit L2"],[#"Org Unit L3"],[#"Org Unit L4"],[#"Org Unit L5"],[#"Org Unit L6"],[#"Org Unit L7"],[#"Org Unit L8"]}),
                " > "
            ),
            type text
        ),

    AddEmailPath =
        Table.AddColumn(
            AddOrgPath,
            "EmailPath",
            each Text.Combine(
                NonEmpty({[#"Manager Email L1"],[#"Manager Email L2"],[#"Manager Email L3"],[#"Manager Email L4"],[#"Manager Email L5"],[#"Manager Email L6"],[#"Manager Email L7"],[#"Manager Email L8"]}),
                " > "
            ),
            type text
        ),

    AddManagerPath =
        Table.AddColumn(
            AddEmailPath,
            "ManagerPath",
            each Text.Combine(
                NonEmpty({[#"Manager L1"],[#"Manager L2"],[#"Manager L3"],[#"Manager L4"],[#"Manager L5"],[#"Manager L6"],[#"Manager L7"],[#"Manager L8"]}),
                " > "
            ),
            type text
        ),

    //--------------------------------------
    // Direct Manager (lowest in hierarchy)
    //--------------------------------------
    AddDirectManager =
        Table.AddColumn(
            AddManagerPath,
            "Direct Manager Name",
            each
                let mgrs = List.RemoveNulls({[#"Manager L8"],[#"Manager L7"],[#"Manager L6"],[#"Manager L5"],[#"Manager L4"],[#"Manager L3"],[#"Manager L2"],[#"Manager L1"]}),
                    direct = if List.Count(mgrs)>0 then List.First(mgrs) else null
                in direct,
            type text
        ),

    AddDirectManagerEmail =
        Table.AddColumn(
            AddDirectManager,
            "Direct Manager Email",
            each
                let mails = List.RemoveNulls({[#"Manager Email L8"],[#"Manager Email L7"],[#"Manager Email L6"],[#"Manager Email L5"],[#"Manager Email L4"],[#"Manager Email L3"],[#"Manager Email L2"],[#"Manager Email L1"]}),
                    directMail = if List.Count(mails)>0 then List.First(mails) else null
                in directMail,
            type text
        )
in
    AddDirectManagerEmail;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_organizational_unit_disciplinary_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_organizational_unit_disciplinary = let
  Source = dim_organizational_unit,
  #"Filtered rows" = Table.SelectRows(Source, each ([OrgUnit type] = "D")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Org Unit ID Composite"})
in
  #"Removed duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_organizational_unit_functional_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_organizational_unit_functional = let
  Source = dim_organizational_unit,
  #"Filtered rows" = Table.SelectRows(Source, each ([OrgUnit type] = "F")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Org Unit ID Composite"})
in
  #"Removed duplicates";
shared dim_organizational_unit_functional_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_organizational_unit_functional", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_organizational_unit_disciplinary_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_organizational_unit_disciplinary", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_month_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_month = let
  Source = dim_date,
  #"Removed columns" = Table.RemoveColumns(Source, {"Date", "Quarter Number", "Quarter Name", "Is Current Month", "Is Previous Month", "Is Current Year"}),
  #"Removed duplicates" = Table.Distinct(#"Removed columns", {"Month Date"})
in
  #"Removed duplicates";
shared dim_month_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_month", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_company_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_company", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_asset_class_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_asset_class = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  Navigation = #"Navigation 2"{[Id = "dim_asset_class", ItemKind = "Table"]}[Data]
in
  Navigation;
shared dim_asset_class_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_asset_class", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
