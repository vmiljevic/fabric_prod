[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data]
in
  #"Navigation 2";
shared StartDate = "2024-12-29" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared EndDate = "2025-05-28" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
[BindToDefaultDestination = true]
shared dates = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Dates", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each Date.FromText([date]) >= Date.FromText(StartDate) and Date.FromText([date]) <= Date.FromText(EndDate))
in
  #"Filtered rows";
