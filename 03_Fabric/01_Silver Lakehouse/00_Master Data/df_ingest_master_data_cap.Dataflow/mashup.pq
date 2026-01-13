[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_vendor_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_vendor = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each [Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/SAP CAP/Vendors/" and [Name] = "2025_Vendors.XLSX"),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13"}),
      #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column2] = "Vendor" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column2] = "Vendor" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed top rows", {{"CoCd", type text}, {"Vendor", type text}, {"Name 1", type text}, {"Cty", type text}, {"PO Box pcd", type text}, {"City", type text}, {"Street", type text}, {"Pers.No.", type text}, {"VAT Registration No.", type text}, {"Tax Number", type text}, {"PayT", type text}, {"DelF", type text}, {"Date", type text}, {"File Name", type text}, {"Refresh Date", type text}})
in
    #"Changed column type";
shared dim_vendor_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_vendor", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
