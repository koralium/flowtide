---
sidebar_position: 8
---

# OpenFGA Connector

## Source

The OpenFGA source allows you to read data from OpenFGA, which can be useful to combine with other data.
It can be combined with the authorization model permission to query parser which creates a view from your model
for a specific permission which can then be materialized with your data.

### Output columns

The source splits up the user and object columns into user_type, user_id, object_type and object_id.
This is done to reduce memory consumption in the system since there is a low cardinality on the type fields, and the values can be reused.

* **user_type** - user type
* **user_id** - identifier of the user
* **relation** - relation name
* **object_type** - object type
* **object_id** - identifier of the object. 

## Authorization model permission to query

It is possible to take in a authorization model and denormalize the permission for a specific type.
This can be useful if one wants to add permisison data to a non relational database such as a search engine to allow searching based on user permissions.

If one develops a CQRS service for instance the query service can get in the materialized permissions from OpenFGA into its database.
It can also be useful if the permissions should be sent to another system that does not integrate with OpenFGA.

Important to note is that one should check the watermark on the stream from OpenFGA to make sure that permissions have been synced to the external system.

To use the model to query parser with sql plan builder, write the following:

```csharp
var modelPlan = OpenFgaToFlowtide.Convert(yourModel, "{type name}", "{relation name}", "{input table name}");
sqlPlanBuilder.AddPlanAsView("permissions", modelPlan);

// Add a source that matches the input table name for openfga
factory.AddOpenFGASource("{input table name}", new OpenFGASourceOptions
{
    ClientConfiguration = clientConfiguration
});
```

The view will contains the following columns:

* user_type - user type field from the tuple
* user_id - identifier of the user
* relation - relation name
* object_type - object type field from the tuple
* object_id - identifier of the object.

You can then use the data from the view to join it with other data.

### Stop at types

It is possible to send in an array of type names where the search should end.
This can be useful in scenarios where say an entire company has access to a resource, it can be better to add the company identifier instead of every single user in the company.

Example:

```csharp
var modelPlan = OpenFgaToFlowtide.Convert(yourModel, "{type name}", "{relation name}", "{input table name}", "company");
```

The relation name will still be the relation name you are filtering on but instead with the object type company and its identifier.

This can work well with using OpenFGA *ListObjects* commands where one could list the companies a user belongs to.
It does require that the application knows more about the authorization model, but can be a good optimization to avoid
alot of user rows.

### How it works

Given the following permission structure:

```
model
  schema 1.1

type user

type group
  relations
    define parent: [group]
    define member: [user]
    define can_read: member or can_read from parent

type doc
  relations
    define group: [group]
    define can_read: can_read from group
```

And one wants to materialize users based on "*doc*: *can_read*" definition, the following query graph is created:


```kroki type=blockdiag
  blockdiag {
    LoopIngress [label = "Loop ingress"];
    LoopEnd [label = "Loop Feedback"];
    FilterMemberRelationGroup [label = "r = 'member' and t = 'group'"];
    ProjectGroupCanRead [label = "project group can_read"];

    GroupCanReadUnion [label = "Union group can_read"];

    FilterCanReadFromGroup [label = "r = 'can_read' and t = 'group'"];
    FilterGroupParent [label = "r = 'parent' and t = 'group'"];
    JoinGroupCanRead [label = "Inner Join on user group and can read"];
    ProjectParentCanRead [label = "project group can_read"];

    FilterMemberGroup [label = "r = 'can_read' and t = 'group'"];
    FilterParentGroup [label = "r = 'group' and t = 'doc'"];
    JoinParentMemberGroup [label = "Inner Join parent with can_read from group"];
    Distinct [label = "Distinct"];
    
    ProjectDocCanRead [label = "Project doc can_read"];

    FinalFilter [label = "filter r = 'can_read' and t = 'project'"];
    OpenFGA [label = "OpenFGA Source"];

    OpenFGA -> LoopIngress;

    LoopIngress -> Distinct;

    Distinct -> FilterMemberRelationGroup;
    FilterMemberRelationGroup -> ProjectGroupCanRead;
    ProjectGroupCanRead -> GroupCanReadUnion;

    Distinct -> FilterCanReadFromGroup;
    Distinct -> FilterGroupParent;
    FilterCanReadFromGroup -> JoinGroupCanRead;
    FilterGroupParent -> JoinGroupCanRead;
    JoinGroupCanRead -> ProjectParentCanRead;
    ProjectParentCanRead -> GroupCanReadUnion;

    GroupCanReadUnion -> LoopEnd;

    Distinct -> FilterMemberGroup; 
    Distinct -> FilterParentGroup;
    FilterMemberGroup -> JoinParentMemberGroup;
    FilterParentGroup -> JoinParentMemberGroup;
    JoinParentMemberGroup -> ProjectDocCanRead;
    ProjectDocCanRead -> LoopEnd;
    
    LoopEnd -> FinalFilter;
    FinalFilter -> Output;
  }
```

Loop feedback will send all events back into the loop ingress. The distinct node directly after the loop ingress is to stop row duplicates to
iterate endlessly, while still allowing rows a high iteration count before terminating.

The default iteration count allowed is 1000 iterations. This is set to stop any potential endless loops.
