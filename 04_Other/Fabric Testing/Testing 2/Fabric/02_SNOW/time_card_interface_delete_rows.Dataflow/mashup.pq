[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_interface_delete_rows_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sys_id", DestinationColumnName = "sys_id"], [SourceColumnName = "week_starts_on", DestinationColumnName = "week_starts_on"], [SourceColumnName = "tc_sys_id", DestinationColumnName = "tc_sys_id"], [SourceColumnName = "comments", DestinationColumnName = "comments"], [SourceColumnName = "postingdate", DestinationColumnName = "postingdate"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "sys_updated_on", DestinationColumnName = "sys_updated_on"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared time_card_interface_delete_rows = let
    // Define the table schema (columns + types)
    EmptyTable = #table(
        type table [
            sys_id = text,
            week_starts_on = text,
            tc_sys_id = text,
            comments = text,
            postingdate = text,
            state = text,
            sys_updated_on = text
        ],
        {}
    )
in
    EmptyTable;
shared time_card_interface_delete_rows_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = "time_card_interface", ItemKind = "Table"]}[Data]
in
  TableNavigation;
