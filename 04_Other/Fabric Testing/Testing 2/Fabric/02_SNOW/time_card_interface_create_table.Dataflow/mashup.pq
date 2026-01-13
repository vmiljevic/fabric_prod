[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_interface_create_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared time_card_interface_create_table = let
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
shared time_card_interface_create_table_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = "time_card_interface", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
