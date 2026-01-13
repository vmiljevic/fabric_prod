section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_cards_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "u_project_name", DestinationColumnName = "u_project_name"], [SourceColumnName = "project_number", DestinationColumnName = "project_number"], [SourceColumnName = "task", DestinationColumnName = "task"], [SourceColumnName = "u_activity_cluster", DestinationColumnName = "u_activity_cluster"], [SourceColumnName = "total", DestinationColumnName = "total"], [SourceColumnName = "week_starts_on", DestinationColumnName = "week_starts_on"], [SourceColumnName = "monday", DestinationColumnName = "monday"], [SourceColumnName = "tuesday", DestinationColumnName = "tuesday"], [SourceColumnName = "wednesday", DestinationColumnName = "wednesday"], [SourceColumnName = "thursday", DestinationColumnName = "thursday"], [SourceColumnName = "friday", DestinationColumnName = "friday"], [SourceColumnName = "saturday", DestinationColumnName = "saturday"], [SourceColumnName = "sunday", DestinationColumnName = "sunday"], [SourceColumnName = "category", DestinationColumnName = "category"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "u_dept_cost_center", DestinationColumnName = "u_dept_cost_center"], [SourceColumnName = "u_prj_unit_resp", DestinationColumnName = "u_prj_unit_resp"], [SourceColumnName = "project_sys_id", DestinationColumnName = "project_sys_id"], [SourceColumnName = "u_timecard_source", DestinationColumnName = "u_timecard_source"], [SourceColumnName = "u_rate_role", DestinationColumnName = "u_rate_role"], [SourceColumnName = "u_project_manager", DestinationColumnName = "u_project_manager"], [SourceColumnName = "u_resource_manager", DestinationColumnName = "u_resource_manager"], [SourceColumnName = "resource_plan", DestinationColumnName = "resource_plan"], [SourceColumnName = "user", DestinationColumnName = "user"], [SourceColumnName = "user_sys_id", DestinationColumnName = "user_sys_id"], [SourceColumnName = "tc_sys_id", DestinationColumnName = "tc_sys_id"], [SourceColumnName = "sys_updated_by", DestinationColumnName = "sys_updated_by"], [SourceColumnName = "sys_updated_on", DestinationColumnName = "sys_updated_on"], [SourceColumnName = "resource_plan_name", DestinationColumnName = "resource_plan_name"], [SourceColumnName = "task_name", DestinationColumnName = "task_name"], [SourceColumnName = "u_company_booking_code_prj", DestinationColumnName = "u_company_booking_code_prj"], [SourceColumnName = "u_company_booking_code_user", DestinationColumnName = "u_company_booking_code_user"], [SourceColumnName = "user_debtor", DestinationColumnName = "user_debtor"], [SourceColumnName = "user_name", DestinationColumnName = "user_name"], [SourceColumnName = "rate", DestinationColumnName = "rate"], [SourceColumnName = "ExportFor", DestinationColumnName = "ExportFor"], [SourceColumnName = "RefreshDate", DestinationColumnName = "RefreshDate"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared time_cards = let
    //
    // ======================
    // PARAMETERS
    // ======================
    //
    startDate = Date.FromText(DateFrom),                 
    nextDate  = Date.AddDays(Date.FromText(DateTo), 1),               

    // Safe daily window: [yesterday 00:00:00 → today 00:00:00)
    startDateText = Date.ToText(startDate, "yyyy-MM-dd") & " 00:00:00",
    nextDateText  = Date.ToText(nextDate,  "yyyy-MM-dd") & " 00:00:00",

    RecordLimit  = Number.FromText(NumberOfRecords),
    CompanyCodes = CompanyCode,
    Debtor       = Debtor,


    //
    // ======================
    // FETCH FUNCTION USING sys_updated_on WINDOW
    // ======================
    //
    fetchUpdatedTimeCards = (filterText as text) =>
        let
finalFilter =
            "sys_updated_on>=javascript:gs.dateGenerate('" &
                Date.ToText(startDate,"yyyy-MM-dd") & "', '00:00:00')" &
            "^sys_updated_on<javascript:gs.dateGenerate('" &
                Date.ToText(nextDate,"yyyy-MM-dd") & "', '00:00:00')" &
            "^" & filterText,


            response =
                Json.Document(
                    Web.Contents(
                        "https://aztech.service-now.com",
                        [
                            RelativePath = "api/now/table/time_card",
                            Query = [
                                sysparm_display_value          = "true",
                                sysparm_exclude_reference_link = "false",
                                sysparm_fields =
                                    "u_project_name,top_task,task,u_activity_cluster,total,week_starts_on," &
                                    "monday,tuesday,wednesday,thursday,friday,saturday,sunday," &
                                    "category,state,u_dept_cost_center,u_prj_unit_resp,u_rep_project," &
                                    "u_timecard_source,u_rate_role,u_project_manager,u_resource_manager," &
                                    "resource_plan,user,sys_id,sys_updated_by,sys_updated_on," &
                                    "resource_plan.short_description,task.short_description," &
                                    "u_dept_cost_center.u_company_booking_code,u_prj_unit_resp.u_company_booking_code," &
                                    "user.u_debtor.u_display_name,user.user_name",
                                sysparm_query = finalFilter,
                                sysparm_limit = Number.ToText(RecordLimit)
                            ],
                            Headers = [Accept="application/json"]
                        ]
                    )
                ),

            tbl      = Table.FromRecords({response}),
            expanded = Table.ExpandListColumn(tbl, "result"),

            records =
                Table.ExpandRecordColumn(
                    expanded,
                    "result",
                    {
                        "u_project_name","top_task","task","u_activity_cluster","total",
                        "week_starts_on","monday","tuesday","wednesday","thursday","friday","saturday","sunday",
                        "category","state","u_dept_cost_center","u_prj_unit_resp","u_rep_project",
                        "u_timecard_source","u_rate_role","u_project_manager","u_resource_manager",
                        "resource_plan","user","sys_id","sys_updated_by","sys_updated_on",
                        "resource_plan.short_description","task.short_description",
                        "u_dept_cost_center.u_company_booking_code","u_prj_unit_resp.u_company_booking_code",
                        "user.u_debtor.u_display_name","user.user_name"
                    },
                    {
                        "u_project_name","top_task","task","u_activity_cluster","total",
                        "week_starts_on","monday","tuesday","wednesday","thursday","friday","saturday","sunday",
                        "category","state","u_dept_cost_center","u_prj_unit_resp","u_rep_project",
                        "u_timecard_source","u_rate_role","u_project_manager","u_resource_manager",
                        "resource_plan","user","tc_sys_id","sys_updated_by","sys_updated_on",
                        "resource_plan.short_description","task.short_description",
                        "u_dept_cost_center.u_company_booking_code","u_prj_unit_resp.u_company_booking_code",
                        "user.u_debtor.u_display_name","user.user_name"
                    }
                ),

            userExpanded =
                Table.ExpandRecordColumn(
                    records,
                    "user",
                    {"display_value", "link"},
                    {"user", "link"}
                ),

            parsed =
                Table.TransformColumns(
                    userExpanded,
                    {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}
                ),

            renamed = Table.RenameColumns(parsed, {{"link", "user_sys_id"}})
        in
            renamed,


    //
    // ======================
    // APPLY 3 FILTER SETS (A/B/C)
    // ======================
    //
    timeCardSetA =
        fetchUpdatedTimeCards(
            "total!=0^categorySTARTSWITHProject^" &
            "top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^" &
            "top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes
        ),

    timeCardSetB =
        fetchUpdatedTimeCards(
            "total!=0^categorySTARTSWITHProject^" &
            "top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^" &
            "user.user_nameISNOTEMPTY^user.u_debtor.sys_id=" & Debtor &
            "^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes
        ),

    timeCardSetC =
        fetchUpdatedTimeCards(
            "total!=0^categoryNOT LIKEProject^" &
            "user.user_nameISNOTEMPTY^user.u_debtor.sys_id=" & Debtor
        ),

    allTimeCards = Table.Combine({timeCardSetA, timeCardSetB, timeCardSetC}),


    //
    // ======================
    // DISTINCT USER IDS
    // ======================
    //
    distinctUserSysIds = List.Distinct(allTimeCards[user_sys_id]),


    //
    // ======================
    // GET RATE CARDS PER USER
    // ======================
    //
    fetchRateCards = (batch as list) =>
        let
            query = "user.sys_idIN" & Text.Combine(batch, ","),

            response =
                Json.Document(
                    Web.Contents(
                        "https://aztech.service-now.com",
                        [
                            RelativePath = "api/now/table/x_ppm_resource_rate_card",
                            Query = [
                                sysparm_display_value          = "true",
                                sysparm_exclude_reference_link = "false",
                                sysparm_fields = "user,co_rate_role,active,sys_created_on",
                                sysparm_query  = query,
                                sysparm_limit  = "5000"
                            ],
                            Headers = [Accept="application/json"]
                        ]
                    )
                ),

            tbl          = Table.FromRecords({response}),
            expandedList = Table.ExpandListColumn(tbl, "result"),

            expanded =
                Table.ExpandRecordColumn(
                    expandedList, "result",
                    {"user","co_rate_role","active","sys_created_on"},
                    {"user","co_rate_role","active","sys_created_on"}),

            roleExpanded =
                Table.ExpandRecordColumn(expanded, "co_rate_role",
                    {"display_value"}, {"rate"}),

            userExpanded =
                Table.ExpandRecordColumn(roleExpanded, "user",
                    {"link"}, {"user_link"}),

            parsed =
                Table.TransformColumns(
                    userExpanded,
                    {{"user_link", each Text.AfterDelimiter(_, "/", 6), type text}}
                ),

            renamed = Table.RenameColumns(parsed, {{"user_link","sys_id"}}),

            cleaned = Table.RemoveColumns(renamed, {"active"}),

            sorted =
                Table.Sort(cleaned,
                    {{"sys_id", Order.Ascending}, {"sys_created_on", Order.Descending}}),

            grouped =
                Table.Group(sorted, {"sys_id"},
                    {{"TopRecord", each Table.FirstN(_, 1)}}),

            final2 =
                Table.ExpandTableColumn(grouped, "TopRecord",
                    {"rate","sys_created_on"})
        in
            final2,

    rateCardBatches = List.Split(distinctUserSysIds, 50),
    allRateCards    = Table.Combine(List.Transform(rateCardBatches, each fetchRateCards(_))),

    allRateCardsClean =
        Table.RenameColumns(
            Table.Distinct(allRateCards, {"sys_id"}),
            {{"sys_id", "rate_sys_id"}}
        ),


    //
    // ======================
    // JOIN TIME CARDS + RATE CARDS
    // ======================
    //
    joinWithRates =
        Table.Join(
            allTimeCards,
            "user_sys_id",
            allRateCardsClean,
            "rate_sys_id",
            JoinKind.LeftOuter
        ),

    joinWithRatesClean =
        Table.RemoveColumns(joinWithRates,
            {"sys_created_on", "rate_sys_id"}),

    withExportInfo =
        Table.AddColumn(joinWithRatesClean, "ExportFor", each CompanyCode),

    base =
        Table.AddColumn(withExportInfo, "RefreshDate", each DateTime.LocalNow()),


    //
    // ======================
    // FULL EXPANSION CHAIN (corrected)
    // ======================
    //
    #"Expanded u_dept_cost_center" =
        Table.ExpandRecordColumn(
            base, "u_dept_cost_center",
            {"display_value","link"},
            {"u_dept_cost_center.display_value","u_dept_cost_center.link"}),

    #"Replaced Errors" =
        Table.ReplaceErrorValues(
            #"Expanded u_dept_cost_center",
            {{"u_dept_cost_center.display_value",""}}),

    #"Removed Columns1" =
        Table.RemoveColumns(#"Replaced Errors", {"u_dept_cost_center.link"}),

    #"Expanded u_rep_project" =
        Table.ExpandRecordColumn(
            #"Removed Columns1", "u_rep_project",
            {"display_value","link"},
            {"u_rep_project.display_value","u_rep_project.link"}),

    #"Extracted Text After Delimiter" =
        Table.TransformColumns(
            #"Expanded u_rep_project",
            {{"u_rep_project.link",
                each Text.AfterDelimiter(_, "/", 6), type text}}),

    #"Renamed Columns" =
        Table.RenameColumns(
            #"Extracted Text After Delimiter",
            {{"u_rep_project.link", "project_sys_id"}}),

    #"Replaced Errors1" = Table.ReplaceErrorValues(#"Renamed Columns", {{"project_sys_id",""}}),

    #"Expanded u_project_manager" =
        Table.ExpandRecordColumn(
            #"Replaced Errors1", "u_project_manager",
            {"display_value","link"},
            {"display_value","link"}),

    #"Renamed Columns2" =
        Table.RenameColumns(#"Expanded u_project_manager",
            {{"display_value","u_project_manager"}}),

    #"Removed Columns" =
        Table.RemoveColumns(#"Renamed Columns2", {"link"}),

    #"Replaced Errors2" =
        Table.ReplaceErrorValues(
            #"Removed Columns",
            {{"u_project_manager",""}}),

    #"Renamed Columns1" =
        Table.RenameColumns(#"Replaced Errors2",
            {{"u_dept_cost_center.display_value","u_dept_cost_center"}}),

    #"Removed Columns2" =
        Table.RemoveColumns(#"Renamed Columns1",
            {"u_rep_project.display_value"}),

    #"Expanded u_prj_unit_resp" =
        Table.ExpandRecordColumn(
            #"Removed Columns2", "u_prj_unit_resp",
            {"display_value","link"},
            {"display_value","link"}),

    // 🔥 CORRECTED BLOCK — only rename display_value here
    #"Renamed Columns3" =
        Table.RenameColumns(#"Expanded u_prj_unit_resp",
            {{"display_value","u_prj_unit_resp"}}),

    // 🔥 NEW VALID STEP — rename the user fields at correct level
    #"Renamed user fields" =
        Table.RenameColumns(#"Renamed Columns3",
            {
                {"user.u_debtor.u_display_name","user_debtor"},
                {"user.user_name","user_name"}
            }
        ),

    #"Removed Columns3" =
        Table.RemoveColumns(#"Renamed user fields", {"link"}),

    #"Replaced Errors3" =
        Table.ReplaceErrorValues(
            #"Removed Columns3",
            {{"u_prj_unit_resp",""}}),

    #"Expanded u_resource_manager" =
        Table.ExpandRecordColumn(
            #"Replaced Errors3", "u_resource_manager",
            {"display_value","link"},
            {"display_value","link"}),

    #"Removed Columns4" =
        Table.RemoveColumns(#"Expanded u_resource_manager", {"link"}),

    #"Renamed Columns4" =
        Table.RenameColumns(#"Removed Columns4",
            {{"display_value","u_resource_manager"}}),

    #"Expanded resource_plan" =
        Table.ExpandRecordColumn(
            #"Renamed Columns4", "resource_plan",
            {"display_value","link"},
            {"display_value","link"}),

    #"Removed Columns5" =
        Table.RemoveColumns(#"Expanded resource_plan", {"link"}),

    #"Expanded task" =
        Table.ExpandRecordColumn(
            #"Removed Columns5", "task",
            {"display_value"}, {"display_value.1"}),

    #"Renamed columns 1" =
        Table.RenameColumns(#"Expanded task",
            {{"display_value.1", "task"}}),

    #"Renamed Columns5" =
        Table.RenameColumns(#"Renamed columns 1",
            {{"display_value","resource_plan"}}),

    #"Replaced Errors4" =
        Table.ReplaceErrorValues(#"Renamed Columns5",
            {{"resource_plan",""}}),

    #"Expanded top_task" =
        Table.ExpandRecordColumn(
            #"Replaced Errors4", "top_task",
            {"display_value","link"},
            {"display_value","link"}),

    #"Removed Columns6" =
        Table.RemoveColumns(#"Expanded top_task", {"link"}),

    #"Replaced Errors5" =
        Table.ReplaceErrorValues(
            #"Removed Columns6",
            {{"display_value",""}}),

    #"Renamed Columns6" =
        Table.RenameColumns(#"Replaced Errors5",
            {{"display_value","project_number"}}),

    #"Transform columns" =
        Table.TransformColumnTypes(#"Renamed Columns6",
            {
                {"u_project_name", type text},{"project_number", type text},{"total", type text},
                {"week_starts_on", type text},{"monday", type text},{"tuesday", type text},
                {"wednesday", type text},{"thursday", type text},{"friday", type text},
                {"saturday", type text},{"sunday", type text},{"category", type text},{"state", type text},
                {"u_dept_cost_center", type text},{"u_prj_unit_resp", type text},{"u_timecard_source", type text},
                {"u_rate_role", type text},{"u_project_manager", type text},{"u_resource_manager", type text},
                {"resource_plan", type text},{"user", type text},{"tc_sys_id", type text},
                {"sys_updated_by", type text},{"sys_updated_on", type text},{"user_name", type text},
                {"rate", type text},{"ExportFor", type text},{"RefreshDate", type text}
            }
        ),

    #"Replace errors" =
        Table.ReplaceErrorValues(#"Transform columns",
            {
                {"u_project_name", null},{"project_number", null},{"total", null},
                {"week_starts_on", null},{"monday", null},{"tuesday", null},{"wednesday", null},
                {"thursday", null},{"friday", null},{"saturday", null},{"sunday", null},
                {"category", null},{"state", null},{"u_dept_cost_center", null},{"u_prj_unit_resp", null},
                {"u_timecard_source", null},{"u_rate_role", null},{"u_project_manager", null},
                {"u_resource_manager", null},{"resource_plan", null},{"user", null},{"tc_sys_id", null},
                {"sys_updated_by", null},{"sys_updated_on", null},{"user_name", null},{"rate", null},
                {"ExportFor", null},{"RefreshDate", null}
            }
        ),

    #"Replaced errors 1" =
        Table.ReplaceErrorValues(#"Replace errors", {{"task",""}}),

    #"Transform columns 1" =
        Table.TransformColumnTypes(#"Replaced errors 1",
            {{"task", type text},{"u_activity_cluster", type text}}),

    #"Replace errors 1" =
        Table.ReplaceErrorValues(#"Transform columns 1",
            {{"task", null},{"u_activity_cluster", null}}),

    #"Renamed columns 2" =
        Table.RenameColumns(#"Replace errors 1",
            {
                {"resource_plan.short_description","resource_plan_name"},
                {"task.short_description","task_name"},
                {"u_dept_cost_center.u_company_booking_code","u_company_booking_code_user"},
                {"u_prj_unit_resp.u_company_booking_code","u_company_booking_code_prj"}
            }
        ),

    #"Transform columns 2" =
        Table.TransformColumnTypes(#"Renamed columns 2",
            {
                {"resource_plan_name", type text},{"task_name", type text},
                {"u_company_booking_code_user", type text},{"u_company_booking_code_prj", type text},
                {"user_debtor", type text}
            }
        ),

    #"Replace errors 2" =
        Table.ReplaceErrorValues(#"Transform columns 2",
            {
                {"resource_plan_name", null},{"task_name", null},{"u_company_booking_code_prj", null},
                {"u_company_booking_code_user", null},{"user_debtor", null}
            }
        )

in
    #"Replace errors 2";
shared CompanyCode = "AT45" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared NumberOfRecords = "9999" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared DateFrom = "2025-11-17" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared DateTo = "2025-11-17" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared Debtor = "6b8aa1d234111200e4851dede6f89c82" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared bcp_time_cards = let
    //
    // ======================
    // PARAMETERS
    // ======================
    //
    startDate = Date.FromText(DateFrom),                  // yesterday
    nextDate  = Date.AddDays(startDate, 1),               // today,

    // Safe daily window: [yesterday 00:00:00 → today 00:00:00)
    startDateText = Date.ToText(startDate, "yyyy-MM-dd") & " 00:00:00",
    nextDateText  = Date.ToText(nextDate,  "yyyy-MM-dd") & " 00:00:00",

    RecordLimit  = Number.FromText(NumberOfRecords),
    CompanyCodes = CompanyCode,
    Debtor       = Debtor,


    //
    // ======================
    // FETCH FUNCTION USING sys_updated_on WINDOW
    // ======================
    //
    fetchUpdatedTimeCards = (filterText as text) =>
        let
            finalFilter =
                "sys_updated_on>=" & startDateText &
                "^sys_updated_on<" & nextDateText &
                "^" & filterText,

            response =
                Json.Document(
                    Web.Contents(
                        "https://aztech.service-now.com",
                        [
                            RelativePath = "api/now/table/time_card",
                            Query = [
                                sysparm_display_value          = "true",
                                sysparm_exclude_reference_link = "false",
                                sysparm_fields =
                                    "u_project_name,top_task,task,u_activity_cluster,total,week_starts_on," &
                                    "monday,tuesday,wednesday,thursday,friday,saturday,sunday," &
                                    "category,state,u_dept_cost_center,u_prj_unit_resp,u_rep_project," &
                                    "u_timecard_source,u_rate_role,u_project_manager,u_resource_manager," &
                                    "resource_plan,user,sys_id,sys_updated_by,sys_updated_on," &
                                    "resource_plan.short_description,task.short_description," &
                                    "u_dept_cost_center.u_company_booking_code,u_prj_unit_resp.u_company_booking_code," &
                                    "user.u_debtor.u_display_name",
                                sysparm_query = finalFilter,
                                sysparm_limit = Number.ToText(RecordLimit)
                            ],
                            Headers = [Accept="application/json"]
                        ]
                    )
                ),

            tbl      = Table.FromRecords({response}),
            expanded = Table.ExpandListColumn(tbl, "result"),

            records =
                Table.ExpandRecordColumn(
                    expanded,
                    "result",
                    {
                        "u_project_name","top_task","task","u_activity_cluster","total",
                        "week_starts_on","monday","tuesday","wednesday","thursday","friday","saturday","sunday",
                        "category","state","u_dept_cost_center","u_prj_unit_resp","u_rep_project",
                        "u_timecard_source","u_rate_role","u_project_manager","u_resource_manager",
                        "resource_plan","user","sys_id","sys_updated_by","sys_updated_on",
                        "resource_plan.short_description","task.short_description",
                        "u_dept_cost_center.u_company_booking_code","u_prj_unit_resp.u_company_booking_code",
                        "user.u_debtor.u_display_name"
                    },
                    {
                        "u_project_name","top_task","task","u_activity_cluster","total",
                        "week_starts_on","monday","tuesday","wednesday","thursday","friday","saturday","sunday",
                        "category","state","u_dept_cost_center","u_prj_unit_resp","u_rep_project",
                        "u_timecard_source","u_rate_role","u_project_manager","u_resource_manager",
                        "resource_plan","user","tc_sys_id","sys_updated_by","sys_updated_on",
                        "resource_plan.short_description","task.short_description",
                        "u_dept_cost_center.u_company_booking_code","u_prj_unit_resp.u_company_booking_code",
                        "user.u_debtor.u_display_name"
                    }
                ),

            userExpanded =
                Table.ExpandRecordColumn(records, "user",
                    {"display_value", "link"},
                    {"user", "link"}),

            parsed =
                Table.TransformColumns(
                    userExpanded,
                    {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}
                ),

            renamed = Table.RenameColumns(parsed, {{"link", "user_sys_id"}})
        in
            renamed,


    //
    // ======================
    // APPLY 3 FILTER SETS (A/B/C)
    // ======================
    //
    timeCardSetA =
        fetchUpdatedTimeCards(
            "state!=Locked^total!=0^categorySTARTSWITHProject^" &
            "top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^" &
            "top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes
        ),

    timeCardSetB =
        fetchUpdatedTimeCards(
            "state!=Locked^total!=0^categorySTARTSWITHProject^" &
            "top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^" &
            "user.user_nameISNOTEMPTY^user.u_debtor.sys_id=" & Debtor &
            "^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes
        ),

    timeCardSetC =
        fetchUpdatedTimeCards(
            "state!=Locked^total!=0^categoryNOT LIKEProject^" &
            "user.user_nameISNOTEMPTY^user.u_debtor.sys_id=" & Debtor
        ),

    allTimeCards = Table.Combine({timeCardSetA, timeCardSetB, timeCardSetC}),


    //
    // ======================
    // DISTINCT USER IDS
    // ======================
    //
    distinctUserSysIds = List.Distinct(allTimeCards[user_sys_id]),


    //
    // ======================
    // GET SYS_USER DETAILS
    // ======================
    //
    fetchUsers = (batch as list) =>
        let
            query = "sys_idIN" & Text.Combine(batch, ","),

            response =
                Json.Document(
                    Web.Contents(
                        "https://aztech.service-now.com",
                        [
                            RelativePath = "api/now/table/sys_user",
                            Query = [
                                sysparm_display_value          = "true",
                                sysparm_exclude_reference_link = "false",
                                sysparm_query = query,
                                sysparm_limit = "5000"
                            ],
                            Headers = [Accept="application/json"]
                        ]
                    )
                ),

            tbl      = Table.FromRecords({response}),
            expanded = Table.ExpandListColumn(tbl, "result"),

            final1 =
                Table.ExpandRecordColumn(expanded, "result",
                    {"user_name","sys_id"})
        in
            final1,

    userBatches = List.Split(distinctUserSysIds, 50),
    allUsers    = Table.Combine(List.Transform(userBatches, each fetchUsers(_))),


    //
    // ======================
    // GET RATE CARDS PER USER
    // ======================
    //
    fetchRateCards = (batch as list) =>
        let
            query = "user.sys_idIN" & Text.Combine(batch, ","),

            response =
                Json.Document(
                    Web.Contents(
                        "https://aztech.service-now.com",
                        [
                            RelativePath = "api/now/table/x_ppm_resource_rate_card",
                            Query = [
                                sysparm_display_value          = "true",
                                sysparm_exclude_reference_link = "false",
                                sysparm_fields = "user,co_rate_role,active,sys_created_on",
                                sysparm_query  = query,
                                sysparm_limit  = "5000"
                            ],
                            Headers = [Accept="application/json"]
                        ]
                    )
                ),

            tbl          = Table.FromRecords({response}),
            expandedList = Table.ExpandListColumn(tbl, "result"),

            expanded =
                Table.ExpandRecordColumn(
                    expandedList, "result",
                    {"user","co_rate_role","active","sys_created_on"},
                    {"user","co_rate_role","active","sys_created_on"}),

            roleExpanded =
                Table.ExpandRecordColumn(expanded, "co_rate_role",
                    {"display_value"}, {"rate"}),

            userExpanded =
                Table.ExpandRecordColumn(roleExpanded, "user",
                    {"link"}, {"user_link"}),

            parsed =
                Table.TransformColumns(
                    userExpanded,
                    {{"user_link", each Text.AfterDelimiter(_, "/", 6), type text}}
                ),

            renamed = Table.RenameColumns(parsed, {{"user_link","sys_id"}}),

            cleaned = Table.RemoveColumns(renamed, {"active"}),

            sorted =
                Table.Sort(cleaned,
                    {{"sys_id", Order.Ascending}, {"sys_created_on", Order.Descending}}),

            grouped =
                Table.Group(sorted, {"sys_id"},
                    {{"TopRecord", each Table.FirstN(_, 1)}}),

            final2 =
                Table.ExpandTableColumn(grouped, "TopRecord",
                    {"rate","sys_created_on"})
        in
            final2,

    rateCardBatches = List.Split(distinctUserSysIds, 50),
    allRateCards    = Table.Combine(List.Transform(rateCardBatches, each fetchRateCards(_))),

    allRateCardsClean =
        Table.RenameColumns(
            Table.Distinct(allRateCards, {"sys_id"}),
            {{"sys_id", "rate_sys_id"}}
        ),


    //
    // ======================
    // JOIN USERS + RATE CARDS
    // ======================
    //
    joinWithUsers =
        Table.Join(
            allTimeCards,
            "user_sys_id",
            allUsers,
            "sys_id",
            JoinKind.LeftOuter
        ),

    joinWithUsersClean =
        Table.RemoveColumns(joinWithUsers, {"sys_id"}),

    joinWithRates =
        Table.Join(
            joinWithUsersClean,
            "user_sys_id",
            allRateCardsClean,
            "rate_sys_id",
            JoinKind.LeftOuter
        ),

    joinWithRatesClean =
        Table.RemoveColumns(joinWithRates,
            {"sys_created_on", "rate_sys_id"}),

    withExportInfo =
        Table.AddColumn(joinWithRatesClean, "ExportFor", each CompanyCode),

    base =
        Table.AddColumn(withExportInfo, "RefreshDate", each DateTime.LocalNow()),


    //
    // ======================
    // FULL EXPANSION CHAIN (UNCHANGED)
    // ======================
    //
    #"Expanded u_dept_cost_center" =
        Table.ExpandRecordColumn(
            base, "u_dept_cost_center",
            {"display_value","link"},
            {"u_dept_cost_center.display_value","u_dept_cost_center.link"}),

    #"Replaced Errors" =
        Table.ReplaceErrorValues(
            #"Expanded u_dept_cost_center",
            {{"u_dept_cost_center.display_value",""}}),

    #"Removed Columns1" =
        Table.RemoveColumns(#"Replaced Errors", {"u_dept_cost_center.link"}),

    #"Expanded u_rep_project" =
        Table.ExpandRecordColumn(
            #"Removed Columns1", "u_rep_project",
            {"display_value","link"},
            {"u_rep_project.display_value","u_rep_project.link"}),

    #"Extracted Text After Delimiter" =
        Table.TransformColumns(
            #"Expanded u_rep_project",
            {{"u_rep_project.link",
                each Text.AfterDelimiter(_, "/", 6), type text}}),

    #"Renamed Columns" =
        Table.RenameColumns(
            #"Extracted Text After Delimiter",
            {{"u_rep_project.link", "project_sys_id"}}),

    #"Replaced Errors1" =
        Table.ReplaceErrorValues(
            #"Renamed Columns", {{"project_sys_id",""}}),

    #"Expanded u_project_manager" =
        Table.ExpandRecordColumn(
            #"Replaced Errors1", "u_project_manager",
            {"display_value","link"},
            {"display_value","link"}),

    #"Renamed Columns2" =
        Table.RenameColumns(#"Expanded u_project_manager",
            {{"display_value","u_project_manager"}}),

    #"Removed Columns" =
        Table.RemoveColumns(#"Renamed Columns2", {"link"}),

    #"Replaced Errors2" =
        Table.ReplaceErrorValues(
            #"Removed Columns",
            {{"u_project_manager",""}}),

    #"Renamed Columns1" =
        Table.RenameColumns(#"Replaced Errors2",
            {{"u_dept_cost_center.display_value","u_dept_cost_center"}}),

    #"Removed Columns2" =
        Table.RemoveColumns(#"Renamed Columns1",
            {"u_rep_project.display_value"}),

    #"Expanded u_prj_unit_resp" =
        Table.ExpandRecordColumn(
            #"Removed Columns2", "u_prj_unit_resp",
            {"display_value","link"},
            {"display_value","link"}),

    #"Renamed Columns3" =
        Table.RenameColumns(#"Expanded u_prj_unit_resp",
            {{"display_value","u_prj_unit_resp"}}),

    #"Removed Columns3" =
        Table.RemoveColumns(#"Renamed Columns3", {"link"}),

    #"Replaced Errors3" =
        Table.ReplaceErrorValues(
            #"Removed Columns3",
            {{"u_prj_unit_resp",""}}),

    #"Expanded u_resource_manager" =
        Table.ExpandRecordColumn(
            #"Replaced Errors3", "u_resource_manager",
            {"display_value","link"},
            {"display_value","link"}),

    #"Removed Columns4" =
        Table.RemoveColumns(#"Expanded u_resource_manager", {"link"}),

    #"Renamed Columns4" =
        Table.RenameColumns(#"Removed Columns4",
            {{"display_value","u_resource_manager"}}),

    #"Expanded resource_plan" =
        Table.ExpandRecordColumn(
            #"Renamed Columns4", "resource_plan",
            {"display_value","link"},
            {"display_value","link"}),

    #"Removed Columns5" =
        Table.RemoveColumns(#"Expanded resource_plan", {"link"}),

    #"Expanded task" =
        Table.ExpandRecordColumn(
            #"Removed Columns5", "task",
            {"display_value"}, {"display_value.1"}),

    #"Renamed columns 1" =
        Table.RenameColumns(#"Expanded task",
            {{"display_value.1", "task"}}),

    #"Renamed Columns5" =
        Table.RenameColumns(#"Renamed columns 1",
            {{"display_value","resource_plan"}}),

    #"Replaced Errors4" =
        Table.ReplaceErrorValues(#"Renamed Columns5",
            {{"resource_plan",""}}),

    #"Expanded top_task" =
        Table.ExpandRecordColumn(
            #"Replaced Errors4", "top_task",
            {"display_value","link"},
            {"display_value","link"}),

    #"Removed Columns6" =
        Table.RemoveColumns(#"Expanded top_task", {"link"}),

    #"Replaced Errors5" =
        Table.ReplaceErrorValues(
            #"Removed Columns6",
            {{"display_value",""}}),

    #"Renamed Columns6" =
        Table.RenameColumns(#"Replaced Errors5",
            {{"display_value","project_number"}}),

    #"Transform columns" =
        Table.TransformColumnTypes(#"Renamed Columns6",
            {
                {"u_project_name", type text},{"project_number", type text},{"total", type text},
                {"week_starts_on", type text},{"monday", type text},{"tuesday", type text},
                {"wednesday", type text},{"thursday", type text},{"friday", type text},
                {"saturday", type text},{"sunday", type text},{"category", type text},{"state", type text},
                {"u_dept_cost_center", type text},{"u_prj_unit_resp", type text},{"u_timecard_source", type text},
                {"u_rate_role", type text},{"u_project_manager", type text},{"u_resource_manager", type text},
                {"resource_plan", type text},{"user", type text},{"tc_sys_id", type text},
                {"sys_updated_by", type text},{"sys_updated_on", type text},{"user_name", type text},
                {"rate", type text},{"ExportFor", type text},{"RefreshDate", type text}
            }
        ),

    #"Replace errors" =
        Table.ReplaceErrorValues(#"Transform columns",
            {
                {"u_project_name", null},{"project_number", null},{"total", null},
                {"week_starts_on", null},{"monday", null},{"tuesday", null},{"wednesday", null},
                {"thursday", null},{"friday", null},{"saturday", null},{"sunday", null},
                {"category", null},{"state", null},{"u_dept_cost_center", null},{"u_prj_unit_resp", null},
                {"u_timecard_source", null},{"u_rate_role", null},{"u_project_manager", null},
                {"u_resource_manager", null},{"resource_plan", null},{"user", null},{"tc_sys_id", null},
                {"sys_updated_by", null},{"sys_updated_on", null},{"user_name", null},{"rate", null},
                {"ExportFor", null},{"RefreshDate", null}
            }
        ),

    #"Replaced errors 1" =
        Table.ReplaceErrorValues(#"Replace errors", {{"task",""}}),

    #"Transform columns 1" =
        Table.TransformColumnTypes(#"Replaced errors 1",
            {{"task", type text},{"u_activity_cluster", type text}}),

    #"Replace errors 1" =
        Table.ReplaceErrorValues(#"Transform columns 1",
            {{"task", null},{"u_activity_cluster", null}}),

    #"Renamed columns 2" =
        Table.RenameColumns(#"Replace errors 1",
            {
                {"resource_plan.short_description","resource_plan_name"},
                {"task.short_description","task_name"},
                {"u_dept_cost_center.u_company_booking_code","u_company_booking_code_user"},
                {"u_prj_unit_resp.u_company_booking_code","u_company_booking_code_prj"}
            }
        ),

    #"Renamed columns 3" =
        Table.RenameColumns(#"Renamed columns 2",
            {{"user.u_debtor.u_display_name","user_debtor"}}),

    #"Transform columns 2" =
        Table.TransformColumnTypes(#"Renamed columns 3",
            {
                {"resource_plan_name", type text},{"task_name", type text},
                {"u_company_booking_code_user", type text},{"u_company_booking_code_prj", type text},
                {"user_debtor", type text}
            }
        ),

    #"Replace errors 2" =
        Table.ReplaceErrorValues(#"Transform columns 2",
            {
                {"resource_plan_name", null},{"task_name", null},{"u_company_booking_code_prj", null},
                {"u_company_booking_code_user", null},{"user_debtor", null}
            }
        ),


    //
    // ======================
    // FINAL FILTER
    // ======================
    //
    #"Filtered rows" =
        Table.SelectRows(#"Replace errors 2", each ([tc_sys_id] <> null))

in
    #"Filtered rows";
shared time_cards_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_ppm_time_cards_daily", ItemKind = "Table"]}[Data]
in
  TableNavigation;
