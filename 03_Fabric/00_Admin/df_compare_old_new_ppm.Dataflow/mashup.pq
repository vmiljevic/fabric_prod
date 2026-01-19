[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_ppm_time_card_interface_daily_dev = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c70edda0-25b6-4c4e-ad60-5a2466e1851f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d6534641-b8b1-49f6-b4c4-5902e79b3a2b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_ppm_time_card_interface_daily", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_ppm_time_cards_daily_dev = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c70edda0-25b6-4c4e-ad60-5a2466e1851f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d6534641-b8b1-49f6-b4c4-5902e79b3a2b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_ppm_time_cards_daily", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"ExportFor"}, {{"Count", each Table.RowCount(_), Int64.Type}})
in
  #"Grouped rows";
shared fact_ppm_time_card_interface_daily_prod = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1bc1173b-7d22-48c2-afa1-74485be57354"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_ppm_time_card_interface_daily", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_ppm_time_cards_daily_prod = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1bc1173b-7d22-48c2-afa1-74485be57354"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_ppm_time_cards_daily", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"ExportFor"}, {{"Count", each Table.RowCount(_), Int64.Type}})
in
  #"Grouped rows";
