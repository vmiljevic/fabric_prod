[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_user_login_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_user_login_data = let
    Source = Csv.Document(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Other/User%20Login%20Data/2025-User-Overall-Last-Login-Report.csv"),[Delimiter=","]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Replaced Value" = Table.ReplaceValue(#"Promoted Headers","0","19001231",Replacer.ReplaceValue,{"Ovl Last Login"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Replaced Value",{{"Nr", Int64.Type}, {"User ID", type text}, {"Last Name", type text}, {"First Name", type text}, {"Full Name", type text}, {"Company", type text}, {"eMail", type text}, {"ALLIANZGRUPPE FQDN", type text}, {"Ovl Last Login", type date}, {"Last Login", type text}, {"Description", type text}, {"Creation Date", Int64.Type}, {"Disabled", type text}, {"Login Expiration", type text}, {"Owner", type text}, {"Master User", type text}, {"Object State", type text}, {"SAP BookingCircle", type text}, {"Hide from GIAM", type text}}),
    #"Added Custom" = Table.AddColumn(#"Changed Type", "Current Date", each DateTime.LocalNow()),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom",{{"Current Date", type date}}),
    #"Added Custom1" = Table.AddColumn(#"Changed Type1", "Days Without Login", each if [Ovl Last Login] < [Current Date] then [Current Date] - [Ovl Last Login] else 0),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom1",{{"Days Without Login", Int64.Type}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type2", each true),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered Rows", {{"Nr", type text}, {"User ID", type text}, {"Last Name", type text}, {"First Name", type text}, {"Full Name", type text}, {"Company", type text}, {"eMail", type text}, {"ALLIANZGRUPPE FQDN", type text}, {"Ovl Last Login", type text}, {"Last Login", type text}, {"Description", type text}, {"Creation Date", type text}, {"Disabled", type text}, {"Login Expiration", type text}, {"Owner", type text}, {"Master User", type text}, {"Object State", type text}, {"SAP BookingCircle", type text}, {"Hide from GIAM", type text}, {"Current Date", type text}, {"Days Without Login", type text}})
in
    #"Changed column type";
shared map_user_login_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_user_login_data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
