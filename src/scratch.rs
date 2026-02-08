use crate::{Context, Database, Dispatch, Entry, FxDashMap, Query};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TypeOf(String);

#[derive(Clone, PartialEq, Eq, Debug)]
enum MyQueries {
    TypeOf(TypeOf),
}

struct MyDatabase {
    type_of: FxDashMap<TypeOf, Entry<String, MyQueries>>,
}

impl Database for MyDatabase {
    type Query = MyQueries;

    fn dispatch<D>(d: D, q: Self::Query) -> D::Result
    where
        D: Dispatch<Self>,
    {
        match q {
            MyQueries::TypeOf(type_of) => d.dispatch(type_of),
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

    fn sub_map(db: &MyDatabase) -> &FxDashMap<TypeOf, Entry<Self::Result, MyQueries>> {
        &db.type_of
    }
}
