[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_employees_at45_archive = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "fact_employees_at45_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared fact_employees_at45_current = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "fact_employees_at45_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared fact_employees_de44_current = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "fact_employees_de44_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared fact_employees_de44_archive = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "fact_employees_de44_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared fact_employees_ce_archive = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "fact_employees_ce_archive", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 3", {{"HR Number", "HR ID"}})
in
  #"Renamed columns";
shared fact_employees_ce_archive_new = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "fact_employees_ce_archive_new", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 3", {{"Work Location - City", "Location"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "HR ID", each if [Company Code] = "HU44" and [EC ID] <> null then [EC ID] else [HR Number]),
  #"Split column by delimiter" = Table.SplitColumn(#"Added custom", "Cost Center Name", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv), {"Cost Center Name.1", "Cost Center Name.2"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Split column by delimiter", {{"Cost Center Name.1", Int64.Type}, {"Cost Center Name.2", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Cost Center Name.1"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Cost Center Name.2", "Cost Center Name"}}),
  #"Split column by delimiter 1" = Table.SplitColumn(#"Renamed columns 1", "Start Date of employment", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv), {"Start Date of employment.1", "Start Date of employment.2"}),
  #"Split column by delimiter 2" = Table.SplitColumn(#"Split column by delimiter 1", "End Date of Employment", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv), {"End Date of Employment.1", "End Date of Employment.2"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Split column by delimiter 2", {"Start Date of employment.2"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"Start Date of employment.1", "Start Date of employment"}, {"End Date of Employment.1", "End Date of Employment"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 2", {"End Date of Employment.2", "HR Number"})
in
  #"Removed columns 2";
shared fact_employees_ce_current = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "fact_employees_ce_current", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 3", {{"Work Location - City", "Location"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "HR ID", each if [Company Code] = "HU44" and [EC ID] <> null then [EC ID] else [HR Number]),
  #"Split column by delimiter" = Table.SplitColumn(#"Added custom", "Cost Center Name", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv), {"Cost Center Name.1", "Cost Center Name.2"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Split column by delimiter", {{"Cost Center Name.1", Int64.Type}, {"Cost Center Name.2", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Cost Center Name.1"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Cost Center Name.2", "Cost Center Name"}}),
  #"Split column by delimiter 1" = Table.SplitColumn(#"Renamed columns 1", "Start Date of employment", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv), {"Start Date of employment.1", "Start Date of employment.2"}),
  #"Split column by delimiter 2" = Table.SplitColumn(#"Split column by delimiter 1", "End Date of Employment", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv), {"End Date of Employment.1", "End Date of Employment.2"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Split column by delimiter 2", {"Start Date of employment.2"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"Start Date of employment.1", "Start Date of employment"}, {"End Date of Employment.1", "End Date of Employment"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 2", {"End Date of Employment.2", "HR Number"})
in
  #"Removed columns 2";
shared _map_grade_proficiency_levels = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_grade_proficiency_level", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared _map_cost_centers_rate_card = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_cost_centers_rate_cards", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared _map_rate_card_names = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_rate_card_sap_translation", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared _map_at45_grades = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_at45_grades", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Lokale Personalnummer", "HR ID"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "GradeKey", each [Year] & "-" & [HR ID]),
  #"Merged queries" = Table.NestedJoin(#"Added custom", {"Grades"}, _map_grade_proficiency_levels, {"Grade"}, "sup_grade_proficiency_levels", JoinKind.LeftOuter),
  #"Expanded sup_grade_proficiency_levels" = Table.ExpandTableColumn(#"Merged queries", "sup_grade_proficiency_levels", {"Proficiency Level"}, {"Proficiency Level"}),
  #"Removed duplicates" = Table.Distinct(#"Expanded sup_grade_proficiency_levels", {"HR ID", "Grades", "Year"})
in
    #"Removed duplicates";
shared _map_at45_master_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_at45_employees_master_data", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Personal Number", "HR ID"}})
in
  #"Renamed columns";
shared _map_rate_card_ce = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_rate_card_ce", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared _map_de44_master_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_de44_employee_master_data", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns( #"Navigation 2", {{"Personal Number", "HR ID"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"HR ID", type text}, {"Year", type text}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "Grade Key", each [Year] & "-" & [HR ID])
in
    #"Added custom";
shared _map_capacity_parameters = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_hr_capacity_parameters", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_employees_at45 = let
  Source = Table.Combine({fact_employees_at45_current, fact_employees_at45_archive}),
  #"Merged queries" = Table.NestedJoin(Source, {"Cost Center Number"}, _map_cost_centers_rate_card, {"Cost Center"}, "sup_cost_centers_rate_card", JoinKind.LeftOuter),
  #"Expanded sup_cost_centers_rate_card" = Table.ExpandTableColumn(#"Merged queries", "sup_cost_centers_rate_card", {"Rate Card", "Invoicing Method"}, {"Rate Card", "Invoicing Method"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded sup_cost_centers_rate_card", {{"Local ID", "HR ID"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns 1", "Grade Key", each [Year] & "-" & [HR ID]),
  #"Replaced value" = Table.ReplaceValue(#"Added custom", 2023, 2024, Replacer.ReplaceValue, {"Grade Key"}),
  #"Merged queries 1" = Table.NestedJoin(#"Replaced value", {"Grade Key"}, _map_at45_grades, {"GradeKey"}, "sup_at45_grades", JoinKind.LeftOuter),
  #"Expanded sup_at45_grades 1" = Table.ExpandTableColumn(#"Merged queries 1", "sup_at45_grades", {"Grades", "Proficiency Level"}, {"Grades", "Proficiency Level"}),
  #"Added custom 1" = Table.AddColumn(#"Expanded sup_at45_grades 1", "Rate Card Full", each if ([Rate Card] <> "Overhead" and [Rate Card] <> null) then [Rate Card] & " - " &[Proficiency Level] else "Overhead"),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 1", {"Rate Card", "Proficiency Level", "Grade Key"}),
  #"Split column by delimiter" = Table.SplitColumn(#"Removed columns", "FunctionalOrgUnit", Splitter.SplitTextByDelimiter(";"), {"FunctionalOrgUnit.1", "FunctionalOrgUnit.2"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Split column by delimiter", {{"FunctionalOrgUnit.1", type text}, {"FunctionalOrgUnit.2", type text}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type", {"FunctionalOrgUnit.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"FunctionalOrgUnit.1", "FunctionalOrgUnit"}, {"HR ID", "HR ID"}, {"HR Manager Personal Number", "Manager HR ID"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns", {"Title", "Gender", "Manager HR ID"}),
  #"Merged queries 2" = Table.NestedJoin(#"Removed columns 2", {"HR ID"}, _map_at45_master_data, {"HR ID"}, "_map_at45_master_data", JoinKind.LeftOuter),
  #"Expanded _map_at45_master_data" = Table.ExpandTableColumn(#"Merged queries 2", "_map_at45_master_data", {"Jobrole NEU", "Contract Type"}, {"Jobrole NEU", "Contract Type"}),
  #"Added custom 2" = Table.AddColumn(#"Expanded _map_at45_master_data", "Role Type New", each if [Jobrole NEU] <> null then [Jobrole NEU] else [Role Type]),
  #"Replaced value 1" = Table.ReplaceValue(#"Added custom 2", null, "Standard", Replacer.ReplaceValue, {"Contract Type"}),
  #"Removed columns 3" = Table.RemoveColumns(#"Replaced value 1", {"Jobrole NEU", "Role Type"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 3", {{"Role Type New", "Role Type"}, {"Grades", "Grade"}})
in
  #"Renamed columns 2";
shared fact_employees_ce = let
  Source = Table.Combine({fact_employees_ce_current, fact_employees_ce_archive, fact_employees_ce_archive_new}),
  #"Added custom" = Table.AddColumn(Source, "Location New", each if [Company Name] = "Allianz Technology SE Hungary Branch" and [Location] = null then "Budapest" else [Location]),
  #"Split column by delimiter" = Table.SplitColumn(#"Added custom", "FunctionalOrgUnit ID", Splitter.SplitTextByDelimiter(";"), {"FunctionalOrgUnit ID.1", "FunctionalOrgUnit ID.2"}),
  #"Removed columns" = Table.RemoveColumns(#"Split column by delimiter", {"FunctionalOrgUnit ID.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"FunctionalOrgUnit ID.1", "FunctionalOrgUnit ID"}}),
  #"Added custom 1" = Table.AddColumn(#"Renamed columns", "Rate Card Key", each [Year] & "-" & [HR ID]),
  #"Merged queries" = Table.NestedJoin(#"Added custom 1", {"Rate Card Key"}, _map_rate_card_ce, {"Rate Card Key"}, "_map_rate_card_ce", JoinKind.LeftOuter),
  #"Expanded _map_rate_card_ce" = Table.ExpandTableColumn(#"Merged queries", "_map_rate_card_ce", {"Rate Card"}, {"Rate Card.1"}),
  #"Added custom 2" = Table.AddColumn(#"Expanded _map_rate_card_ce", "Rate Card Full", each if [Rate Card] = null then [Rate Card.1] else [Rate Card]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 2", {{"FunctionalOrgUnit ID", type text}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type", { "EC ID", "Rate Card.1"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns 1", null, "Overhead-Aut", Replacer.ReplaceValue, {"Rate Card Full"}),
  #"Added custom 3" = Table.AddColumn(#"Replaced value", "Invoicing Method New", each if ([Invoicing Method] = null and ([Rate Card Full] = "Overhead" or [Rate Card Full] = "Overhead-Aut" ))then "Overhead" else "T&M"),
  #"Removed columns 2" = Table.RemoveColumns(#"Added custom 3", {"Invoicing Method", "Rate Card", "Location"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 2", {{"Location New", "Location"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 1", {"Head Count"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 3", {{"Active Head Count", "AHC"}, {"Full time Employee", "FTE"}, {"Start Date of employment", "Start Date"}, {"End Date of Employment", "End Date"}, {"HR Manager Name", "HR Manager"}, {"HR Manager _email address_", "HR Manager Email"}}),
  #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 2", {"HR Manager Email", "Functional Manager _email address_", "Functional Manager"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 4", {{"DisciplinaryOrgUnit ID", "DisciplinaryOrgUnit"}, {"FunctionalOrgUnit ID", "FunctionalOrgUnit"}}),
  #"Removed columns 5" = Table.RemoveColumns(#"Renamed columns 3", {"Rate Card Key", "Gender", "Date of Birth"}),
  #"Added custom 4" = Table.AddColumn(#"Renamed columns 4", "HR ID Corrected", each if ([User ID] = "HWIZ96U") or ([User ID] = "ALMAP") then [HR ID] & "_1" else [HR ID]),
  #"Removed columns 6" = Table.RemoveColumns(#"Added custom 4", {"HR ID"}),
  #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 6", {{"HR ID Corrected", "HR ID"}}),
  #"Changed column type 1" = Table.TransformColumns(#"Removed columns 5",{{"Start Date", each if _ = null or _ = "" then null else try Date.From(_) otherwise null, type date},{"End Date", each if _ = null or _ = "" then null else try Date.From(_) otherwise null, type date}}),
    #"Renamed columns 4" = Table.RenameColumns(#"Removed columns 5", {{"Team Name", "Role Type"}, {"Invoicing Method New", "Invoicing Method"}, {"Cost Center", "Cost Center Number"}})
in
  #"Renamed columns 4";
shared fact_employees_de44 = let
  Source = Table.Combine({fact_employees_de44_current, fact_employees_de44_archive}),
  #"Renamed columns" = Table.RenameColumns(Source, {{"Local ID", "HR ID"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "Grade Key", each [Year] & "-" & [HR ID]),
  #"Merged queries" = Table.NestedJoin(#"Added custom", {"Grade Key"}, _map_de44_master_data, {"Grade Key"}, "_map_de44_master_data", JoinKind.LeftOuter),
  #"Expanded _map_de44_master_data" = Table.ExpandTableColumn(#"Merged queries", "_map_de44_master_data", {"Email", "Grade", "Gender", "User ID", "Role Type", "Functional OrgUnit ID", "Disciplinary OrgUnit ID", "Position", "Contract Type", "My HR Manager"}, {"Email", "Grade", "Gender", "User ID", "Role Type", "Functional OrgUnit ID", "Disciplinary OrgUnit ID", "Position.1", "Contract Type", "My HR Manager"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded _map_de44_master_data", {{"Grade", type text}}),
  #"Merged queries 1" = Table.NestedJoin(#"Changed column type", {"Grade"}, _map_grade_proficiency_levels, {"Grade"}, "_map_grade_proficiency_levels", JoinKind.LeftOuter),
  #"Expanded _map_grade_proficiency_levels" = Table.ExpandTableColumn(#"Merged queries 1", "_map_grade_proficiency_levels", {"Proficiency Level"}, {"Proficiency Level"}),
  #"Merged queries 2" = Table.NestedJoin(#"Expanded _map_grade_proficiency_levels", {"Cost Center Number"}, _map_cost_centers_rate_card, {"Cost Center"}, "_map_cost_centers_rate_card", JoinKind.LeftOuter),
  #"Expanded _map_cost_centers_rate_card" = Table.ExpandTableColumn(#"Merged queries 2", "_map_cost_centers_rate_card", {"Rate Card", "Invoicing Method"}, {"Rate Card", "Invoicing Method"}),
  #"Added custom 1" = Table.AddColumn(#"Expanded _map_cost_centers_rate_card", "Rate Card Full", each if ([Rate Card] <> "Overhead" and [Rate Card] <> null) then [Rate Card] & " - " &[Proficiency Level] else "Overhead"),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 1", {"Birthday Date", "Grade Key", "Gender", "Position"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Position.1", "Position"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"Proficiency Level", "Rate Card"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"My HR Manager", "HR Manager"}, {"Disciplinary OrgUnit ID", "DisciplinaryOrgUnit"}, {"Functional OrgUnit ID", "FunctionalOrgUnit"}}),
  #"Added custom 2" = Table.AddColumn(#"Renamed columns 2", "Location", each "Munich"),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "End Date", each ""),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 3", {{"FTE", Currency.Type}}),
  #"Divided column" = Table.TransformColumns(#"Changed column type 1", {{"FTE", each _ / 100, Currency.Type}})
in
  #"Divided column";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_employee_internal_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_employee_internal = let
  Source = Table.Combine({fact_employees_ce, fact_employees_de44, fact_employees_at45}),
  #"Trimmed text" = Table.TransformColumns(Source, {{"Rate Card Full", each Text.Trim(Text.From(_)), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"Rate Card Full", each Text.Clean(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Cleaned text", {"Rate Card Full"}, _map_rate_card_names, {"Material Name Corrected"}, "_map_rate_card_names", JoinKind.LeftOuter),
  #"Expanded _map_rate_card_names" = Table.ExpandTableColumn(#"Merged queries", "_map_rate_card_names", {"Material", "Material Name", "Rate Role"}, {"Material", "Material Name", "Rate Role"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded _map_rate_card_names", {{"Material Name", "Rate Card Name"}, {"Material", "Rate Card Material Number"}, {"Rate Role", "Rate Card Role"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "Rate Card Final", each if [Rate Card Name] = null then [Rate Card Full] else [Rate Card Name]),
  #"Removed columns" = Table.RemoveColumns(#"Added custom", {"Rate Card Name", "Rate Card Full"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Rate Card Final", "Rate Card Name"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns 1", null, "Standard", Replacer.ReplaceValue, {"Contract Type"}),
  #"Added custom 1" = Table.AddColumn(#"Replaced value", "Month Date", each   [Month] & "-" & [Year]),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Cost Center Number and Name", each [Cost Center Number] & " - " & [Cost Center Name]),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Added custom 2", {{"FTE", type text}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Changed column type 2", ",", ".", Replacer.ReplaceText, {"FTE"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 1", {{"Start Date", type date}, {"End Date", type date}, {"Month Date", type date}, {"Refresh Date", type datetime}, {"AHC", Int64.Type}, {"FTE", Currency.Type}, {"User ID", type text}, {"Position", type text}, {"Role Type", type text}, {"Email", type text}, {"HR Manager", type text}, {"DisciplinaryOrgUnit", type text}, {"FunctionalOrgUnit", type text}, {"HR ID", type text}, {"Location", type text}, {"Invoicing Method", type text}, {"Contract Type", type text}, {"Rate Card Material Number", type text}, {"Rate Card Role", type text}, {"Rate Card Name", type text}, {"Cost Center Number and Name", type text}}),
  #"Added custom 3" = Table.AddColumn(#"Changed column type", "DisciplinaryOrgUnit Key", each [Year] & [Month] & "-" & [DisciplinaryOrgUnit]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "FunctionalOrgUnit Key", each [Year] & [Month] & "-" & [FunctionalOrgUnit]),
  #"Added custom 5" = Table.AddColumn(#"Added custom 4", "Employee Name", each [First Name] & " " & [Last Name]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 5", {{"File Name", type text}, {"DisciplinaryOrgUnit Key", type text}, {"FunctionalOrgUnit Key", type text}, {"Employee Name", type text}}),
  #"Added custom 6" = Table.AddColumn(#"Changed column type 1", "Rate Key", each [Company Code] & "-" & [Rate Card Material Number]),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Added custom 6", {{"Rate Key", type text}}),
  #"Merged queries 1" = Table.NestedJoin(#"Changed column type 3", {"Rate Key"}, _map_rate_cards_sap, {"Rate Key Short"}, "_map_rate_cards_sap", JoinKind.LeftOuter),
  #"Expanded _map_rate_cards_sap" = Table.ExpandTableColumn(#"Merged queries 1", "_map_rate_cards_sap", {"Rate per Day"}, {"Rate per Day"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Expanded _map_rate_cards_sap", {{"Rate per Day", "Daily Rate"}}),
  #"Added custom 7" = Table.AddColumn(#"Renamed columns 2", "Rate Key PPM Bookings", each [Company Code] & "-" & [Month] & "-" & [Year] & "-" & [HR ID]),
  #"Renamed columns 3" = Table.RenameColumns(#"Added custom 7", {{"Rate Key PPM Bookings", "Employee Key"}}),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Renamed columns 3", {{"Employee Key", type text}}),
  #"Merged queries 2" = Table.NestedJoin(#"Changed column type 4", {"Company Code"}, _map_capacity_parameters, {"Sales Organisation"}, "_map_capacity_parameters", JoinKind.LeftOuter),
  #"Expanded _map_capacity_parameters" = Table.ExpandTableColumn(#"Merged queries 2", "_map_capacity_parameters", {"Annual Capacity in PDs"}, {"Annual Capacity in PDs"}),
  #"Changed column type 6" = Table.TransformColumnTypes(#"Expanded _map_capacity_parameters", {{"Annual Capacity in PDs", Int64.Type}}),
  #"Added custom 8" = Table.AddColumn(#"Changed column type 6", "Available HR PDs", each [FTE] * ([Annual Capacity in PDs]/12)),
  #"Removed columns 1" = Table.RemoveColumns(#"Added custom 8", {"Annual Capacity in PDs"}),
  #"Changed column type 5" = Table.TransformColumnTypes(#"Removed columns 1", {{"Available HR PDs", Currency.Type}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type 5", {"Employee Key"})
in
  #"Removed duplicates";
shared _map_rate_cards_sap = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dim_rate_card", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared fact_employee_internal_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_employee_internal", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
