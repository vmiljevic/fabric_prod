[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_ppm_sys_users_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_ppm_sys_users = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Filtered rows" = Table.SelectRows(LH_Table, each Text.Contains([Id], "users") and Text.Contains([Id], "dim_ppm_") and (Text.Contains([Id], "at45") or Text.Contains([Id], "de44") or Text.Contains([Id], "sk45") or Text.Contains([Id], "sk44") or Text.Contains([Id], "cz45") or Text.Contains([Id], "hu44")) ),

    // Convert lakehouse metadata table → list of records
    LH_List = Table.ToRecords(#"Filtered rows"),

    // Load each table via its [Data] field
    LoadedTables = List.Transform(LH_List, each _[Data]),

    // Combine all fact_ppm_* tables
    Combined = Table.Combine(LoadedTables),
  #"Filtered rows 1" = Table.SelectRows(Combined, each [sys_id] <> null)
in
    #"Filtered rows 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_ppm_project_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_ppm_project = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Filtered rows" = Table.SelectRows(LH_Table, each Text.Contains([Id], "project") and Text.Contains([Id], "fact_ppm_") and (Text.Contains([Id], "at45") or Text.Contains([Id], "de44") or Text.Contains([Id], "sk45") or Text.Contains([Id], "sk44") or Text.Contains([Id], "cz45") or Text.Contains([Id], "hu44")) ),

    // Convert lakehouse metadata table → list of records
    LH_List = Table.ToRecords(#"Filtered rows"),

    // Load each table via its [Data] field
    LoadedTables = List.Transform(LH_List, each _[Data]),

    // Combine all fact_ppm_* tables
    Combined = Table.Combine(LoadedTables),
  #"Filtered rows 1" = Table.SelectRows(Combined, each [sys_id] <> null)
in
    #"Filtered rows 1";
shared dim_ppm_project_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_ppm_project", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_ppm_accounting_element_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_ppm_accounting_element = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Filtered rows" = Table.SelectRows(LH_Table, each Text.Contains([Id], "accounting_element") and Text.Contains([Id], "dim_ppm_") and (Text.Contains([Id], "at45") or Text.Contains([Id], "de44") or Text.Contains([Id], "sk45") or Text.Contains([Id], "sk44") or Text.Contains([Id], "cz45") or Text.Contains([Id], "hu44")) ),

    // Convert lakehouse metadata table → list of records
    LH_List = Table.ToRecords(#"Filtered rows"),

    // Load each table via its [Data] field
    LoadedTables = List.Transform(LH_List, each _[Data]),

    // Combine all fact_ppm_* tables
    Combined = Table.Combine(LoadedTables)
in
    Combined;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_ppm_external_rate_cards_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_ppm_external_rate_cards = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Filtered rows" = Table.SelectRows(LH_Table, each Text.Contains([Id], "external_rate_cards") and Text.Contains([Id], "dim_ppm_") and (Text.Contains([Id], "at45") or Text.Contains([Id], "de44") or Text.Contains([Id], "sk45") or Text.Contains([Id], "sk44") or Text.Contains([Id], "cz45") or Text.Contains([Id], "hu44")) ),

    // Convert lakehouse metadata table → list of records
    LH_List = Table.ToRecords(#"Filtered rows"),

    // Load each table via its [Data] field
    LoadedTables = List.Transform(LH_List, each _[Data]),

    // Combine all fact_ppm_* tables
    Combined = Table.Combine(LoadedTables),
  #"Filtered rows 1" = Table.SelectRows(Combined, each [pol_sys_id] <> null)
in
    #"Filtered rows 1";
shared dim_ppm_accounting_element_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_ppm_accounting_element", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_ppm_sys_users_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_ppm_sys_users", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_ppm_external_rate_cards_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_ppm_external_rate_cards", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
