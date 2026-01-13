[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Query = let
  ResolvedValue = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)", "__NOT_RESOLVED__"),
  Custom = #table(
       {"RunTimeCheck"},
       {{ResolvedValue}}
   ),
  #"Changed column type" = Table.TransformColumnTypes(Custom, {{"RunTimeCheck", type text}})
in
  #"Changed column type";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c70edda0-25b6-4c4e-ad60-5a2466e1851f"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d6534641-b8b1-49f6-b4c4-5902e79b3a2b"]}[Data],
  TableNavigation = Navigation_2{[Id = "test_variable", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
