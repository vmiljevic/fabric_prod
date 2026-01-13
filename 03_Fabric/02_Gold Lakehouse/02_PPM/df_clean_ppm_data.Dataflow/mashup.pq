[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared dim_ppm_external_rate_cards_unique = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_ppm_external_rate_cards", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([user_sys_id] <> null)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"display_value"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"po_number", "PO Number"}, {"u_demand_id", "Demand Id"}, {"u_transfer_status", "Transfer Status"}, {"status", "Status"}, {"u_vendor_name", "Vendor Name"}, {"u_wbs_element", "WBS Element"}, {"u_planning_wbs_element", "Planning WBS Element"}, {"pol_number", "PO Line Number"}, {"rate_card_display", "Rate Card Name"}, {"user_display", "User Name"}, {"rate_per_hour_eur", "Rate per Hour"}}),
  #"Sorted rows" = Table.Sort(#"Renamed columns", {{"PO Number", Order.Ascending}}),
  #"Added custom" = Table.AddColumn(#"Sorted rows", "Rate Key", each [u_purchase_no] & "-" & [user_sys_id]),
  #"Removed duplicates" = Table.Distinct(#"Added custom", {"Rate Key"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed duplicates", {{"Rate per Hour", Currency.Type}}),
  #"Added custom 1" = Table.AddColumn(#"Changed column type", "Daily Rate", each Number.RoundDown([Rate per Hour] * 8, 0)),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"Daily Rate", type number}})
in
  #"Changed column type 1";
shared dim_ppm_accounting_element = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_ppm_accounting_element", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"accounting_master", "Accounting Master"}, {"u_wbs_element_id", "Element Id"}, {"u_element_approved", "Element Status"}, {"u_expense_type", "Expense Type"}, {"u_name", "Element Name"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "Poject Key", each [ExportFor] & "-" & [project_sys_id])
in
  #"Added custom";
shared dim_ppm_accounting_element_capex = let
  Source = dim_ppm_accounting_element,
  #"Filtered rows" = Table.SelectRows(Source, each ([Expense Type] = "Capex"))
in
  #"Filtered rows";
shared dim_ppm_sys_users = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_ppm_sys_users", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Export for", "ExportFor"}})
in
  #"Renamed columns";
shared dim_ppm_sys_users_unique = let
  Source = dim_ppm_sys_users,
  #"Removed duplicates" = Table.Distinct(Source, {"sys_id"})
in
  #"Removed duplicates";
shared fact_at45_jmp_nonproductive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_at45_jmp_nonproductive", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Month", type date}}),
  #"Lowercased text" = Table.TransformColumns(#"Changed column type", {{"employee email", each Text.Lower(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Lowercased text", {"Month", "employee email"}, fact_employee_internal, {"Month Date", "Email"}, "fact_employee_internal", JoinKind.LeftOuter),
  #"Expanded fact_employee_internal 1" = Table.ExpandTableColumn(#"Merged queries", "fact_employee_internal", {"User ID", "Company Code", "HR Manager", "Employee Name","HR ID", "Rate Card Material Number","Rate Card Name", "Daily Rate"}, {"User ID", "Company Code", "HR Manager", "Employee Name","HR ID","Material", "Rate Card Name","Daily Rate"}),
  #"Filtered rows 1" = Table.SelectRows(#"Expanded fact_employee_internal 1", each ([HR ID] <> null)),
  #"Renamed Columns1" = Table.RenameColumns(#"Filtered rows 1", {{"Employee Name", "Booked By"}, {"Hours", "Booked Hours"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed Columns1", ",", ".", Replacer.ReplaceText, {"Booked Hours"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value", {{"Booked Hours", type number}}),
  #"Added Custom" = Table.AddColumn(#"Changed column type 1", "Booked PDs", each [Booked Hours] / 8),
  #"Changed Type" = Table.TransformColumnTypes(#"Added Custom", {{"Booked PDs", type number}}),
  #"Added Custom1" = Table.AddColumn(#"Changed Type", "State", each "Processed"),
  #"Renamed Columns2" = Table.RenameColumns(#"Added Custom1", {{"Activity", "Booking Category"}}),
  #"Added Custom2" = Table.AddColumn(#"Renamed Columns2", "Project / Activity Name", each [Booking Category]),
  #"Added Custom3" = Table.AddColumn(#"Added Custom2", "Booking Date", each [Month]),
  #"Added Custom4" = Table.AddColumn(#"Added Custom3", "ExportFor", each "AT45"),
  #"Renamed Columns3" = Table.RenameColumns(#"Added Custom4", {{"HR Manager", "Resource Manager"}}),
  #"Added Custom5" = Table.AddColumn(#"Renamed Columns3", "Resource Type", each "Internal Resources"),
  #"Removed Columns" = Table.RemoveColumns(#"Added Custom5", {"employee email"}),
  #"Added custom 1" = Table.AddColumn(#"Removed Columns", "Time Card Source", each "JMP Direct"),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Project Group", each "6 - Non-Chargable"),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Amount", each [Booked PDs] * [Daily Rate]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Daily Rate Charged", each [Daily Rate]),
  #"Added custom 5" = Table.AddColumn(#"Added custom 4", "Rate Key", each [Company Code] & "-" & [Material]),
  #"Added custom 6" = Table.AddColumn(#"Added custom 5", "Employee Key", each [Company Code] & "-" & Date.ToText([Month], [Format="MM", Culture="de-DE"]) & "-"& Date.ToText([Month], [Format="yyyy", Culture="de-DE"]) &"-" & [HR ID]),
  #"Renamed columns" = Table.RenameColumns(#"Added custom 6", {{"Month", "Month Date"}, {"Company Code", "Company Code User"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns", {{"Daily Rate Charged", Currency.Type}, {"Amount", Currency.Type}, {"Daily Rate", Currency.Type}}),
  #"Added custom 7" = Table.AddColumn(#"Changed column type 2", "SAP State Final", each "0 - Non Chargable"),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Added custom 7", {{"SAP State Final", type text}, {"Employee Key", type text}, {"Project Group", type text}, {"Time Card Source", type text}, {"Resource Type", type text}, {"ExportFor", type text}, {"Booking Date", type date}, {"Project / Activity Name", type text}, {"State", type text}, {"Rate Key", type text}})
in
  #"Changed column type 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_ppm_project_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_ppm_project = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_ppm_project", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"short_description", "Project Name"}, {"project_number", "Project Number"}, {"accounting_master", "Accounting Master"}, {"u_project_state", "Project State"}, {"u_project_category", "Project Category"}, {"u_bill_code", "Bill Code"}, {"u_ppm_debtor_relationship", "Debtor Relationship"}, {"u_profit_center", "Profit Center"}, {"primary_portfolio", "Project Portfolio"}, {"percent_complete", "Percentage Complete"}, {"project_manager", "Project Manager"}, {"u_project_manager_deputee", "Project Manager Deputy"}, {"status", "Status"}, {"u_complexity", "Complexity"}, {"demand", "Demand Name"}, {"u_project_controller", "Project Controller"}, {"u_project_sponsor", "Project Sponsor"}, {"start_date", "Start Date"}, {"end_date", "End Date"}, {"u_az_group_project_id", "AZ Group Project Id"}, {"u_ppm_request_envelope_id", "Demand Number"}, {"u_company_code", "Company Code"}}),
  #"Added custom 2" = Table.AddColumn(#"Renamed columns", "Project Key", each [ExportFor] & "-" & [sys_id]),
  #"Merged queries" = Table.NestedJoin(#"Added custom 2", {"Project Key"}, dim_ppm_accounting_element_capex, {"Poject Key"}, "dim_ppm_accounting_element_capex", JoinKind.LeftOuter),
  #"Expanded dim_ppm_accounting_element_capex" = Table.ExpandTableColumn(#"Merged queries", "dim_ppm_accounting_element_capex", {"Expense Type"}, {"Expense Type"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded dim_ppm_accounting_element_capex", null, "Not Capitalized", Replacer.ReplaceValue, {"Expense Type"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "Capex", "Capitalized", Replacer.ReplaceText, {"Expense Type"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Replaced value 1", {{"Expense Type", "Capitalization Indicator"}}),
  #"Added custom 1" = Table.AddColumn(#"Renamed columns 2", "Custom", each if ([Bill Code] = "Billing to Customer ( T& M )" or [Bill Code] = "Multi Debtor") then
            "1 - Billing to Customer (T&M)"
        else if ([Capitalization Indicator] = "Capitalized" and [Project Category] = "Change / Strategic") then
            "5 - IT Assets"
        else if ([Project Category] = "Maintenance Support (WA)" and [ExportFor] = [Company Code]) then
            "3 - IT Services Run"
        else if ([Bill Code] <> "Billing to Customer ( T& M )" and [Project Category] = "Maintenance Change (PF)") then
            "4 - IT Service Internal Change"
        else if ([Bill Code] <> "Billing to Customer ( T& M )" and [ExportFor] <> [Company Code]) then
            "2 - Internal Billing ICC (T&M)"
        else
            "Other"),
  #"Renamed columns 1" = Table.RenameColumns(#"Added custom 1", {{"Custom", "Project Group"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns 1", "Debtor", each if [Debtor Relationship] = "" and [Company Code] = "0044" then "1200000024 Allianz Technology SE" 
        else if [Debtor Relationship] = "" and [Company Code] = "AT45"  then "2000009339 Allianz Technology GmbH"
        else if [Debtor Relationship] = "" and [Company Code] = "UK44" then "2000009173 Allianz Technology SE UK"
        else if [Debtor Relationship] = "" and [Company Code] = "AU44" then "2000011963 Allianz Technology SE Australi"
        else if [Debtor Relationship] = "" and [Company Code] = "CZ45" then "1300000069 Allianz Pojistovna, a.s."
        else if [Debtor Relationship] = "" and [Company Code] = "HU44" then "1300000064 Allianz Hungária Biztosító Zrt"
        else if [Debtor Relationship] = "" and [Company Code] = "SK44" then "1300000004 Allianz-Slovenská Poistovna a."
        else if [Debtor Relationship] = "" and [Company Code] = "SK45" then "1300000004 Allianz-Slovenská Poistovna a."
        else [Debtor Relationship]),
  #"Removed duplicates" = Table.Distinct(#"Added custom", {"Project Key"}),
  #"Merged queries 1" = Table.NestedJoin(#"Removed duplicates", {"Accounting Master"}, dim_wbs, {"WBS Element"}, "dim_wbs", JoinKind.LeftOuter),
  #"Expanded dim_wbs" = Table.ExpandTableColumn(#"Merged queries 1", "dim_wbs", {"Change Run Indicator"}, {"Change Run Indicator"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded dim_wbs", {{"Project Name", type text}, {"Start Date", type datetime}, {"End Date", type datetime}, {"Debtor", type text}, {"Project Group", type text}, {"Project Key", type text}}),
  #"Added custom 3" = Table.AddColumn(#"Changed column type", "Year Service", each if Date.Year([End Date]) >= Date.Year(DateTime.LocalNow()) then Text.From(Date.Year(DateTime.LocalNow())) else Text.From(Date.Year([End Date]))),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Project Number and Name", each [Project Number] & "-" & [Project Name]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 4", {{"Year Service", type text}, {"Project Number and Name", type text}, {"Start Date", type date}, {"End Date", type date}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_ppm_time_card_interface_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_ppm_time_card_interface = let
    //--------------------------------------------
    // Load fact table
    //--------------------------------------------
    srcLakehouse = Lakehouse.Contents(null),
    srcWorkspace = srcLakehouse{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    srcData = srcWorkspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
    factRaw = srcData{[Id = "fact_ppm_time_card_interface", ItemKind = "Table"]}[Data],
  #"Removed duplicates" = Table.Distinct(factRaw, {"tc_sys_id"}),

    factSlim =
        Table.SelectColumns(
            #"Removed duplicates",
            {"tc_sys_id","ExportFor","project_sys_id","posting_date","state","comments","week_starts_on","rate_per_hour", "raterole","ponumber","currency"}
        ),
    factFiltered = Table.SelectRows(factSlim, each [tc_sys_id] <> ""),
    factTyped = Table.TransformColumnTypes(factFiltered, {{"posting_date", type datetime},{"state", type text}}),

    //--------------------------------------------
    // Buffer dimensions
    //--------------------------------------------
    dimProjectBuf = Table.Buffer(dim_ppm_project),
    mapBillingBuf = Table.Buffer(_map_billing_calendar),

    //--------------------------------------------
    // Merge project info
    //--------------------------------------------
    factWithKey = Table.AddColumn(factTyped, "Project Key", each [ExportFor] & "-" & [project_sys_id]),
    factMerged = Table.NestedJoin(factWithKey, {"Project Key"}, dimProjectBuf, {"Project Key"}, "prj", JoinKind.LeftOuter),
    factExpanded = Table.ExpandTableColumn(factMerged, "prj", {"Project Group"}, {"Project Group"}),

    //--------------------------------------------
    // Normalize SAP columns
    //--------------------------------------------
    factRenamed = Table.RenameColumns(factExpanded, {{"posting_date","SAP Posting Date"},{"state","SAP State"}}),
    factDate = Table.TransformColumnTypes(factRenamed, {{"SAP Posting Date", type date}}),

    //--------------------------------------------
    // Prepare billing calendar (safe date conversion)
    //--------------------------------------------
    today = Date.From(DateTime.LocalNow()),

    calFixed =
        Table.TransformColumns(
            mapBillingBuf,
            {
                {"Date From", each try Date.From(DateTime.FromText(Text.From(_))) otherwise null, type date},
                {"Date To", each try Date.From(DateTime.FromText(Text.From(_))) otherwise null, type date},
                {"Date From ICC", each try Date.From(DateTime.FromText(Text.From(_))) otherwise null, type date},
                {"Date To ICC", each try Date.From(DateTime.FromText(Text.From(_))) otherwise null, type date}
            }
        ),
    calFiltered = Table.SelectRows(calFixed, each [Date To] < today),

    //--------------------------------------------
    // Expand normal billing ranges
    //--------------------------------------------
    calNormAdd = Table.AddColumn(
        calFiltered,"DateListNorm",
        each if [Date From]<>null and [Date To]<>null
             then List.Dates([Date From],Duration.Days([Date To]-[Date From])+1,#duration(1,0,0,0))
             else {}
    ),
    calNormExpanded = Table.ExpandListColumn(calNormAdd,"DateListNorm"),
    calNormTyped = Table.TransformColumnTypes(calNormExpanded,{{"DateListNorm",type date}}),

    //--------------------------------------------
    // Expand ICC billing ranges
    //--------------------------------------------
    calICCAdd = Table.AddColumn(
        calFiltered,"DateListICC",
        each if [Date From ICC]<>null and [Date To ICC]<>null
             then List.Dates([Date From ICC],Duration.Days([Date To ICC]-[Date From ICC])+1,#duration(1,0,0,0))
             else {}
    ),
    calICCExpanded = Table.ExpandListColumn(calICCAdd,"DateListICC"),
    calICCTyped = Table.TransformColumnTypes(calICCExpanded,{{"DateListICC",type date}}),

    calNormBuf = Table.Buffer(calNormTyped),
    calICCBuf  = Table.Buffer(calICCTyped),

    //--------------------------------------------
    // Join for T&M (Normal)
    //--------------------------------------------
    joinNormal =
        Table.NestedJoin(
            factDate, {"SAP Posting Date"},
            Table.SelectColumns(calNormBuf, {"DateListNorm", "Month Date"}),
            {"DateListNorm"},
            "joinNormal",
            JoinKind.LeftOuter
        ),
    expNormal = Table.ExpandTableColumn(joinNormal,"joinNormal",{"Month Date"},{"Billing_Normal"}),

    //--------------------------------------------
    // Join for ICC (non-T&M)
    //--------------------------------------------
    joinICC =
        Table.NestedJoin(
            expNormal, {"SAP Posting Date"},
            Table.SelectColumns(calICCBuf, {"DateListICC", "Month Date"}),
            {"DateListICC"},
            "joinICC",
            JoinKind.LeftOuter
        ),
    expICC = Table.ExpandTableColumn(joinICC,"joinICC",{"Month Date"},{"Billing_ICC"}),

    //--------------------------------------------
    // Billing Month logic
    //--------------------------------------------
    addBillingMonth = Table.AddColumn(
        expICC,"Billing Month",
        each if [SAP State]<>"Rejected" and [Project Group]="1 - Billing to Customer (T&M)" then [Billing_Normal]
             else if [SAP State]<>"Rejected" and [Project Group]<>"1 - Billing to Customer (T&M)" then [Billing_ICC]
             else null
    ),

    //--------------------------------------------
    // SAP State logic
    //--------------------------------------------
    addSAPStateFinal = Table.AddColumn(
        addBillingMonth,"SAP State Display",
        each if ([SAP State]=null or [SAP State]="") then "1 - Not Transfered to SAP"
             else if ( [SAP State]="Successful" or [SAP State]="Processed") and ([Billing Month]=null or [Billing Month]="")
                  then "4 - Booked in SAP"
             else if ( [SAP State]="Successful" or [SAP State]="Processed") and not ([Billing Month]=null or [Billing Month]="")
                  then "5 - Charged in SAP"
             else if [SAP State]="Pending"  then "3 - Booking in SAP Pending"
             else if [SAP State]="Rejected" then "2 - SAP Booking Rejected"
             else "6 - SAP State Unclear"
    ),

    //--------------------------------------------
    // Cleanup
    //--------------------------------------------
    cleaned = Table.RemoveColumns(addSAPStateFinal,{"Billing_Normal","Billing_ICC","week_starts_on"}),
    typed = Table.TransformColumnTypes(cleaned,{{"Billing Month",type date},{"SAP State Display",type text}}),
    renamed = Table.RenameColumns(typed,{{"comments","Comments"}}),
  #"Changed column type" = Table.TransformColumnTypes(renamed, {{"Project Key", type text}, {"rate_per_hour", Currency.Type}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_ppm_time_cards_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}, PreviewOnlySteps = {"Filtered rows 2"}]
shared fact_ppm_time_cards = let
    Source = Lakehouse.Contents(null),
    #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Id = "fact_ppm_time_cards", ItemKind = "Table"]}[Data],
  #"Filtered rows 2" = Table.SelectRows(#"Navigation 3", each Text.StartsWith([week_starts_on], "2025-02")),

    #"Renamed columns" = Table.RenameColumns(
        #"Filtered rows 2",
        {
            {"task", "Project Task"},
            {"u_activity_cluster", "Activity Cluster"},
            {"total", "Total Hours"},
            {"week_starts_on", "Week Starts On"},
            {"monday", "Monday"},
            {"tuesday", "Tuesday"},
            {"wednesday", "Wednesday"},
            {"thursday", "Thursday"},
            {"friday", "Friday"},
            {"saturday", "Saturday"},
            {"sunday", "Sunday"},
            {"category", "Booking Category"},
            {"state", "State"},
            {"u_dept_cost_center", "User Cost Center and Name"},
            {"u_prj_unit_resp", "Project Costs Center and Name"},
            {"u_timecard_source", "Time Card Source"},
            {"u_rate_role", "Rate Role"},
            {"u_project_manager", "Project Manager"},
            {"u_resource_manager", "Resource Manager"},
            {"resource_plan", "Resource Plan Number"},
            {"user", "Booked By"},
            {"resource_plan_name", "Resource Plan Name"},
            {"task_name", "Task Name"},
            {"user_name", "User ID"},
            {"rate", "Rate Card"},
            {"u_project_name", "Project Name"},
            {"project_number", "Project Number"}
        }
    ),

    #"Added custom" = Table.AddColumn(#"Renamed columns", "Project Key", each [ExportFor] & "-" & [project_sys_id]),

    #"Merged queries" = Table.NestedJoin(#"Added custom", {"Project Key"}, dim_ppm_project, {"Project Key"}, "dim_ppm_project", JoinKind.LeftOuter),
    #"Expanded dim_ppm_project" = Table.ExpandTableColumn(#"Merged queries", "dim_ppm_project", {"Project Group"}, {"Project Group"}),

    #"Added custom 1" = Table.AddColumn(#"Expanded dim_ppm_project", "Project Group New", each if [Project Group] = null then "6 - Non-Chargable" else [Project Group]),
    #"Removed columns" = Table.RemoveColumns(#"Added custom 1", {"Project Group"}),
    #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Project Group New", "Project Group"}}),

    #"Merged queries 1" = Table.NestedJoin(#"Renamed columns 1", {"user_sys_id"}, dim_ppm_sys_users_unique, {"sys_id"}, "dim_ppm_sys_users_unique", JoinKind.LeftOuter),
    #"Expanded dim_ppm_sys_users_unique" = Table.ExpandTableColumn(#"Merged queries 1", "dim_ppm_sys_users_unique", {"u_local_logon_name", "u_preferred_logon_name"}, {"u_local_logon_name", "u_preferred_logon_name"}),

    #"Replaced value" = Table.ReplaceValue(#"Expanded dim_ppm_sys_users_unique", "_PAD01", "", Replacer.ReplaceText, {"User ID"}),

    #"Added custom 2" = Table.AddColumn(
        #"Replaced value",
        "User Id new",
        each
            if Text.Contains([User ID], "_") then
                if [u_local_logon_name] <> null and [u_local_logon_name] <> "" then [u_local_logon_name]
                else if [u_preferred_logon_name] <> null and [u_preferred_logon_name] <> "" then [u_preferred_logon_name]
                else [User ID]
            else [User ID]
    ),

    #"Changed column type" = Table.TransformColumnTypes(#"Added custom 2", {{"User Id new", type text}}),
    #"Replaced value 1" = Table.ReplaceValue(#"Changed column type", "DEACT_", "", Replacer.ReplaceText, {"User Id new"}),

    #"Split column by delimiter" = Table.SplitColumn(#"Replaced value 1", "User Id new", Splitter.SplitTextByEachDelimiter({"_"}, QuoteStyle.Csv, true), {"User Id new.1", "User Id new.2"}),

    #"Removed columns 1" = Table.RemoveColumns(#"Split column by delimiter", {"User Id new.2", "u_preferred_logon_name", "u_local_logon_name", "User ID"}),
    #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"User Id new.1", "User ID"}}),
    #"Changed column type 15" = Table.TransformColumnTypes(#"Renamed columns 2", {{"Week Starts On", type date}}),
  #"Uppercased text 1" = Table.TransformColumns(#"Changed column type 15", {{"User ID", each Text.Upper(_), type nullable text}}),

    #"Added custom 6" = Table.AddColumn(#"Uppercased text 1", "Date 1", each [Week Starts On]),
    #"Added custom 7" = Table.AddColumn(#"Added custom 6", "Date 2", each Date.AddDays([Week Starts On], 1)),
    #"Added custom 8" = Table.AddColumn(#"Added custom 7", "Date 3", each Date.AddDays([Week Starts On], 2)),
    #"Added custom 9" = Table.AddColumn(#"Added custom 8", "Date 4", each Date.AddDays([Week Starts On], 3)),
    #"Added custom 10" = Table.AddColumn(#"Added custom 9", "Date 5", each Date.AddDays([Week Starts On], 4)),
    #"Added custom 11" = Table.AddColumn(#"Added custom 10", "Date 6", each Date.AddDays([Week Starts On], 5)),
    #"Added custom 12" = Table.AddColumn(#"Added custom 11", "Date 7", each Date.AddDays([Week Starts On], 6)),

    #"Unpivoted only selected columns" = Table.Unpivot(#"Added custom 12", {"Date 1", "Date 2", "Date 3", "Date 4", "Date 5", "Date 6", "Date 7"}, "Attribute", "Value"),
    #"Unpivoted only selected columns 1" = Table.Unpivot(#"Unpivoted only selected columns", {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}, "Attribute (2)", "Value (2)"),

    #"Renamed columns 5" = Table.RenameColumns(#"Unpivoted only selected columns 1", {{"Value", "Booking Date"}}),
    #"Filtered rows" = Table.SelectRows(#"Renamed columns 5", each ([#"Value (2)"] <> "0")),
    #"Renamed columns 6" = Table.RenameColumns(#"Filtered rows", {{"Value (2)", "Booked Hours"}}),

    #"Added custom 14" = Table.AddColumn(#"Renamed columns 6", "Booking Day New", each Date.DayOfWeekName([Booking Date], "en-US")),
    #"Renamed columns 7" = Table.RenameColumns(#"Added custom 14", {{"Attribute (2)", "Booking Day"}}),
    #"Filtered rows 1" = Table.SelectRows(#"Renamed columns 7", each [Booking Day] = [Booking Day New]),

    #"Changed column type 4" = Table.TransformColumnTypes(#"Filtered rows 1", {{"Booked Hours", Currency.Type}}),
    #"Added custom 13" = Table.AddColumn(#"Changed column type 4", "Booked PDs", each [Booked Hours] / 8),
    #"Changed column type 5" = Table.TransformColumnTypes(#"Added custom 13", {{"Booked PDs", Currency.Type}}),

    #"Removed columns 2" = Table.RemoveColumns(#"Changed column type 5", {"Attribute", "Booking Day", "Booking Day New", "Total Hours"}),
    #"Changed column type 9" = Table.TransformColumnTypes(#"Removed columns 2", {{"Booking Date", type date}}),

    #"Added custom 20" = Table.AddColumn(
        #"Changed column type 9",
        "Month Date",
        each Date.ToText([Booking Date], [Format = "MM", Culture = "de-DE"]) & "-" &
             Date.ToText([Booking Date], [Format = "yyyy", Culture = "de-DE"])
    ),

    #"Added custom 3" = Table.AddColumn(#"Added custom 20", "Employee Key", each [ExportFor] & "-" & [Month Date] & "-" & [User ID]),

    #"Merged queries 2" = Table.NestedJoin(#"Added custom 3", {"Employee Key"}, fact_employee_internal, {"User Key"}, "fact_employee_internal", JoinKind.LeftOuter),
    #"Expanded fact_employee_internal" = Table.ExpandTableColumn(#"Merged queries 2", "fact_employee_internal", {"HR ID"}, {"HR ID"}),

    #"Added custom 4" = Table.AddColumn(
        #"Expanded fact_employee_internal",
        "Custom",
        each
            if [Rate Card] = null then "External Resources"
            else if [HR ID] = null then "External Resources - Other Location"
            else "Internal Resources"
    ),

    #"Renamed columns 3" = Table.RenameColumns(#"Added custom 4", {{"Custom", "Resource Type"}}),

    #"Merged queries 4" = Table.NestedJoin(#"Renamed columns 3", {"Rate Card"}, _map_rate_cards_translation, {"Material Name PPM"}, "_map_rate_cards_translation", JoinKind.LeftOuter),
    #"Expanded _map_rate_cards_translation" = Table.ExpandTableColumn(#"Merged queries 4", "_map_rate_cards_translation", {"Material", "Material Name"}, {"Material", "Material Name"}),

    #"Merged queries 7" = Table.NestedJoin(#"Expanded _map_rate_cards_translation", {"tc_sys_id"}, fact_ppm_time_card_interface, {"tc_sys_id"}, "fact_ppm_time_card_interface", JoinKind.LeftOuter),
    #"Expanded fact_ppm_time_card_interface" = Table.ExpandTableColumn(#"Merged queries 7", "fact_ppm_time_card_interface", {"SAP State Display","rate_per_hour","ponumber","raterole","currency"}, {"SAP State Display","rate_per_hour","ponumber","raterole","currency"}),
  #"Added custom 25" = Table.AddColumn(#"Expanded fact_ppm_time_card_interface", "External Rate Key", each [ponumber] & "-" & [user_sys_id]),

    #"Merged queries 3" = Table.NestedJoin(#"Added custom 25", {"External Rate Key"}, dim_ppm_external_rate_cards_unique, {"Rate Key"}, "dim_ppm_external_rate_cards_unique", JoinKind.LeftOuter),
    #"Expanded dim_ppm_external_rate_cards_unique" = Table.ExpandTableColumn(#"Merged queries 3", "dim_ppm_external_rate_cards_unique", {"Daily Rate"}, {"Daily Rate External"}),
  #"Added custom 24" = Table.AddColumn(#"Expanded dim_ppm_external_rate_cards_unique", "Daily Rate External Final", each if [rate_per_hour] <> null and [currency] <> "HUF" then [rate_per_hour]*8 else [Daily Rate External]),
  #"Removed columns 8" = Table.RemoveColumns(#"Added custom 24", {"rate_per_hour", "Daily Rate External"}),
  #"Renamed columns 12" = Table.RenameColumns(#"Removed columns 8", {{"Daily Rate External Final", "Daily Rate External"}}),

    #"Added custom 21" = Table.AddColumn(
        #"Renamed columns 12",
        "SAP State Final",
        each
            if [SAP State Display] = null and [Project Group] = "6 - Non-Chargable" then "0 - Non Chargable"
            else if [SAP State Display] = null and [Project Group] <> "6 - Non-Chargable" then "1 - Not Transfered to SAP"
            else [SAP State Display]
    ),

    #"Changed column type 8" = Table.TransformColumnTypes(#"Added custom 21", {{"SAP State Final", type text}}),

    #"Renamed columns 4" = Table.RenameColumns(#"Changed column type 8", {{"Material Name", "Rate Card Name"}}),
    #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns 4", {{"Material", type text}}),

    #"Added custom 5" = Table.AddColumn(
        #"Changed column type 1",
        "Rate Key",
        each
            if [u_company_booking_code_user] <> null and [u_company_booking_code_user] <> "" then
                [u_company_booking_code_user] & "-" & [Material]
            else
                [ExportFor] & "-" & [Material]
    ),

    #"Changed column type 2" = Table.TransformColumnTypes(#"Added custom 5", {{"Rate Key", type text}}),
    #"Replaced value 2" = Table.ReplaceValue(#"Changed column type 2", "0044", "I044", Replacer.ReplaceText, {"Rate Key"}),

    #"Merged queries 5" = Table.NestedJoin(#"Replaced value 2", {"Rate Key"}, dim_rate_card, {"Rate Key Short"}, "dim_rate_card", JoinKind.LeftOuter),
    #"Expanded dim_rate_card 1" = Table.ExpandTableColumn(#"Merged queries 5", "dim_rate_card", {"Currency", "Rate per Day"}, {"Currency Other Location", "Daily Rate Other Locations"}),

    #"Renamed columns 9" = Table.RenameColumns(#"Expanded dim_rate_card 1", {{"Daily Rate Other Locations", "Daily Rate Internal"}}),

    #"Merged queries 6" = Table.NestedJoin(#"Renamed columns 9", {"Currency Other Location"}, _map_exchange_rate, {"Currency"}, "_map_exchange_rate", JoinKind.LeftOuter),
    #"Expanded _map_exchange_rate" = Table.ExpandTableColumn(#"Merged queries 6", "_map_exchange_rate", {"EUR"}, {"EUR"}),

    #"Added custom 15" = Table.AddColumn(#"Expanded _map_exchange_rate", "Daily Rate Internal in EUR", each [Daily Rate Internal] / [EUR]),
    #"Changed column type 6" = Table.TransformColumnTypes(#"Added custom 15", {{"Daily Rate Internal in EUR", Currency.Type}}),

    #"Changed column type 3" = Table.TransformColumnTypes(#"Changed column type 6", {{"Week Starts On", type date}}),

    #"Added custom 16" = Table.AddColumn(
        #"Changed column type 3",
        "Daily Rate Final",
        each if [Resource Type] = "External Resources" then [Daily Rate External] else [Daily Rate Internal in EUR]
    ),

    #"Changed column type 7" = Table.TransformColumnTypes(#"Added custom 16", {{"Daily Rate Final", Currency.Type}}),

    #"Removed columns 4" = Table.RemoveColumns(#"Changed column type 7", {"Daily Rate Internal in EUR", "EUR", "Daily Rate Internal", "Currency Other Location", "Daily Rate External"}),
    #"Renamed columns 8" = Table.RenameColumns(#"Removed columns 4", {{"Daily Rate Final", "Daily Rate"}}),

    #"Added custom 17" = Table.AddColumn(
        #"Renamed columns 8",
        "Employee Key New",
        each
            if [Resource Type] = "Internal Resources" then
                [ExportFor] & "-" & [Month Date] & "-" & [HR ID]
            else
                [ExportFor] & "-" & [Month Date] & "-" & [User ID]
    ),

    #"Changed column type 10" = Table.TransformColumnTypes(
        #"Added custom 17",
        {
            {"Employee Key", type text},
            {"Project Group", type text},
            {"Project Key", type text},
            {"RefreshDate", type datetime}
        }
    ),

    #"Renamed columns 10" = Table.RenameColumns(
        #"Changed column type 10",
        {
            {"u_company_booking_code_prj", "Company Code Project"},
            {"u_company_booking_code_user", "Company Code User"}
        }
    ),

    #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 10", {"Rate Card"}),
    #"Changed column type 11" = Table.TransformColumnTypes(#"Removed columns 3", {{"sys_updated_on", type datetime}}),

    #"Removed columns 5" = Table.RemoveColumns(#"Changed column type 11", {"Employee Key"}),
    #"Replaced value 3" = Table.ReplaceValue(#"Removed columns 5", null, 0, Replacer.ReplaceValue, {"Daily Rate"}),

    #"Added custom 19" = Table.AddColumn(
        #"Replaced value 3",
        "Time Card Link",
        each if [tc_sys_id] <> "" then
            "https://aztech.service-now.com/nav_to.do?uri=%2Ftime_card.do%3Fsys_id%3D" & [tc_sys_id] & "%26"
        else ""
    ),

    #"Changed column type 13" = Table.TransformColumnTypes(#"Added custom 19", {{"Time Card Link", type text}, {"Resource Type", type text}}),
    #"Changed column type 14" = Table.TransformColumnTypes(#"Changed column type 13", {{"Month Date", type date}}),

    #"Uppercased text" = Table.TransformColumns(#"Changed column type 14", {{"Employee Key New", each Text.Upper(_), type nullable text}}),
    #"Renamed columns 11" = Table.RenameColumns(#"Uppercased text", {{"Employee Key New", "Employee Key"}}),


  #"Added custom 22" = Table.AddColumn(#"Renamed columns 11", "Project / Activity Name", each if [Booking Category] = "Project/Project Task" then [Project Number] & "-" & [Project Name] else [Booking Category] ),
  #"Removed columns 6" = Table.RemoveColumns(#"Added custom 22", {"SAP State Display"}),
  #"Changed column type 16" = Table.TransformColumnTypes(#"Removed columns 6", {{"Project / Activity Name", type text}}),
  #"Merged queries 8" = Table.NestedJoin(#"Changed column type 16", {"Resource Type"}, _map_rate_charged, {"Resource Type"}, "_map_rate_charged", JoinKind.LeftOuter),
  #"Expanded _map_rate_charged" = Table.ExpandTableColumn(#"Merged queries 8", "_map_rate_charged", {"Addon1", "Addon2"}, {"Addon1", "Addon2"}),
  #"Added custom 23" = Table.AddColumn(#"Expanded _map_rate_charged", "Daily Rate Charged", each ([Daily Rate] * [Addon1])*[Addon2]),

    #"Added custom 18" = Table.AddColumn(#"Added custom 23", "Amount", each if [Resource Type] = "External Resources - Other Location" then [Booked PDs] * [Daily Rate Charged] else [Booked PDs] * [Daily Rate] ),
    #"Changed column type 12" = Table.TransformColumnTypes(#"Added custom 18", {{"Amount", Currency.Type}, {"Daily Rate Charged", type number}}),
  #"Removed columns 7" = Table.RemoveColumns(#"Changed column type 12", {"Addon1", "Addon2"}),
  #"Changed column type 17" = Table.TransformColumnTypes(#"Removed columns 7", {{"Daily Rate Charged", Currency.Type}}),
  #"Appended query" = Table.Combine({#"Changed column type 17", fact_at45_jmp_nonproductive})
in
    #"Appended query";
shared fact_employee_internal = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_employee_internal", ItemKind = "Table"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 2", "User Key", each [Company Code] & "-" & Date.ToText([Month Date], [Format="MM", Culture="de-DE"]) & "-" & Date.ToText([Month Date], [Format="yyyy", Culture="de-DE"]) & "-" & [User ID]),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Employee Key New", each [Company Code] & "-" & [Month] & "-" & [Year] & "-" & [HR ID]),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 1", {"Employee Key"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Employee Key New", "Employee Key"}}),
  #"Lowercased text" = Table.TransformColumns(#"Renamed columns", {{"Email", each Text.Lower(_), type nullable text}})
in
  #"Lowercased text";
shared dim_wbs = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_wbs", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_rate_card = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_rate_card", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"Currency", Order.Ascending}}),
  #"Removed duplicates" = Table.Distinct(#"Sorted rows", {"Rate Key Short"})
in
  #"Removed duplicates";
shared _map_billing_calendar = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_billing_run_calendar", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Date From", type date}, {"Date To", type date}, {"Date From ICC", type date}, {"Date To ICC", type date}, {"Month Date", type date}})
in
  #"Changed column type";
shared _map_rate_cards_translation = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_rate_card_sap_translation", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared _map_exchange_rate = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_exchange_rates", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"EUR", Currency.Type}})
in
  #"Changed column type";
shared dim_ppm_project_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_ppm_project", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_ppm_time_cards_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_ppm_time_cards", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_ppm_time_card_interface_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_ppm_time_card_interface", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared _map_rate_charged = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_rate_charged", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Addon1", type number}, {"Addon2", type number}})
in
  #"Changed column type";
