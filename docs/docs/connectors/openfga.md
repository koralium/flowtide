---
sidebar_position: 8
---

# OpenFGA Connector



## Authorization model permission to query

It is possible to take in a authorization model and denormalize the permission for a specific type.
This can be useful if one wants to add permisison data to a non relational database such as a search engine to allow searching based on user permissions.

If one develops a CQRS service for instance the query service can get in the materialized permissions from OpenFGA into its database.
It can also be useful if the permissions should be sent to another system that does not integrate with OpenFGA.

Important to note is that one should check the watermark on the stream from OpenFGA to make sure that permissions have been synced to the external system.

To use the model to query parser with sql plan builder, write the following:

```csharp
var modelPlan = new FlowtideOpenFgaModelParser(yourModel).Parse("{type name}", "{relation name}", "{input table name}");
sqlPlanBuilder.AddPlanAsView("permissions", modelPlan);

// Add a source that matches the input table name for openfga
factory.AddOpenFGASource("{input table name}", new OpenFGASourceOptions
{
    ClientConfiguration = clientConfiguration
});
```

The view will contains the following columns:

* User - user field from the tuple
* Relation - relation name
* Object - object field from the tuple

You can then use the data from the view to join it with other data.

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

And one wants to materialize users based on "*project*: *can_read*" definition, the following query graph is created:


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
