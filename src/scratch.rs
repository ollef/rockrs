use crate::{Context, Database, EntryStatus, FxDashMap, Queries, Query};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TypeOf(String);

#[derive(Clone, PartialEq, Eq, Debug)]
enum MyQueries {
    TypeOf(TypeOf),
}

struct MyDatabase {
    type_of: FxDashMap<TypeOf, EntryStatus<(String, Vec<MyQueries>)>>,
}

impl Database for MyDatabase {
    type Query = MyQueries;
}

impl Queries<MyDatabase> for MyQueries {
    fn try_fetch(qc: &Context<MyDatabase>, query: MyQueries) -> EntryStatus<()> {
        match query {
            MyQueries::TypeOf(q) => TypeOf::try_fetch(qc, q).map(|_| ()),
        }
    }
}

impl Query<MyDatabase> for TypeOf {
    type Result = String;

    fn inject(self) -> MyQueries {
        MyQueries::TypeOf(self)
    }

    fn rule(_: &Context<MyDatabase>, query: &TypeOf) -> Self::Result {
        format!("Type of term: {}", query.0)
    }

    fn try_fetch(qc: &Context<MyDatabase>, query: TypeOf) -> EntryStatus<String> {
        qc.try_fetch_dash_map(query, &qc.database.type_of)
    }
}
