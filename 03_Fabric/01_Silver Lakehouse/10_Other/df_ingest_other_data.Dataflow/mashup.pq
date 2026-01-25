[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_user_login_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_user_login_data = let
    Source = Csv.Document(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Other/User%20Login%20Data/2025-User-Overall-Last-Login-Report.csv"),[Delimiter=","]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Replaced Value" = Table.ReplaceValue(#"Promoted Headers","0","19001231",Replacer.ReplaceValue,{"Ovl Last Login"}),
  #"Removed other columns" = Table.SelectColumns(#"Replaced Value", {"Nr", "User ID", "Last Name", "First Name", "Company", "eMail", "Ovl Last Login", "Creation Date"}),
    #"Added Custom" = Table.AddColumn(#"Removed other columns", "Current Date", each DateTime.LocalNow()),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom", {{"Current Date", type date}, {"Ovl Last Login", type date}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Changed Type1", null, #date(1900, 1, 1), Replacer.ReplaceValue, {"Ovl Last Login"}),
    #"Added Custom1" = Table.AddColumn(#"Replaced value 1", "Days Without Login", each if [Ovl Last Login] < [Current Date] then [Current Date] - [Ovl Last Login] else 0),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom1",{{"Days Without Login", Int64.Type}})
in
    #"Changed Type2";
shared map_user_login_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_user_login_data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
