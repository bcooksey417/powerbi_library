let
    Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion=48]),
    User1 = Source{[Name="User"]}[Data]
in
    User1