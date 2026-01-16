[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared lakehouse_finance_gold_dev = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c70edda0-25b6-4c4e-ad60-5a2466e1851f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "6c07b77b-2af9-48eb-95e5-87b904a26384"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 1", each ([Schema] = "dbo"))
in
  #"Filtered rows";
shared lakehouse_finance_gold_prod = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a1aaa939-0dd1-4de5-804b-126e55a75f88"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 1", each ([Schema] = "dbo"))
in
  #"Filtered rows";
shared lakehouse_finance_silver_prod = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1bc1173b-7d22-48c2-afa1-74485be57354"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 1", each ([Schema] = "dbo") and ([ItemKind] = "Table"))
in
  #"Filtered rows";
shared lakehouse_finance_silver_dev = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c70edda0-25b6-4c4e-ad60-5a2466e1851f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d6534641-b8b1-49f6-b4c4-5902e79b3a2b"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 1", each [Schema] = "dbo")
in
  #"Filtered rows";
shared lakehouse_finance_silver_prod_merged = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1bc1173b-7d22-48c2-afa1-74485be57354"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 1", each ([Schema] = "dbo")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"Id"}, lakehouse_finance_silver_dev, {"Id"}, "lakehouse_finance_silver_dev", JoinKind.LeftOuter),
  #"Expanded lakehouse_finance_silver_dev" = Table.ExpandTableColumn(#"Merged queries", "lakehouse_finance_silver_dev", {"Id"}, {"Id.1"}),
  #"Filtered rows 1" = Table.SelectRows(#"Expanded lakehouse_finance_silver_dev", each ([Id.1] = null))
in
  #"Filtered rows 1";
shared lakehouse_finance_silver_dev_merged = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c70edda0-25b6-4c4e-ad60-5a2466e1851f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d6534641-b8b1-49f6-b4c4-5902e79b3a2b"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 1", each ([Schema] = "dbo")),
  #"Merged queries" = Table.NestedJoin(#"Filtered rows", {"Id"}, lakehouse_finance_silver_prod, {"Id"}, "lakehouse_finance_silver_prod", JoinKind.LeftOuter),
  #"Expanded lakehouse_finance_silver_prod" = Table.ExpandTableColumn(#"Merged queries", "lakehouse_finance_silver_prod", {"Id"}, {"Id.1"})
in
  #"Expanded lakehouse_finance_silver_prod";
