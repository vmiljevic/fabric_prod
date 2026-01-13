[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared dates = let
    // ---- Parameters ----
    StartDate = Date.FromText(StartDate),    // Change as needed
    EndDate   = Date.FromText(EndDate),  // Change as needed

    // ---- Find first Sunday on/after StartDate ----
    FirstSunday = Date.AddDays(
        StartDate,
        Number.Mod(7 - Date.DayOfWeek(StartDate, Day.Sunday), 7)
    ),

    // ---- Generate Sundays until EndDate ----
    Sundays = List.Generate(
        () => FirstSunday,
        each _ <= EndDate,
        each Date.AddDays(_, 7)
    ),

    // ---- Convert to Table and format as yyyy-MM-dd ----
    SundayTable = Table.TransformColumns(
        Table.FromList(Sundays, Splitter.SplitByNothing(), {"date"}),
        {{"date", each Date.ToText(_, "yyyy-MM-dd"), type text}}
    )
in
    SundayTable;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data]
in
  #"Navigation 2";
shared StartDate = "2024-12-29" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared EndDate = "2025-12-28" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
