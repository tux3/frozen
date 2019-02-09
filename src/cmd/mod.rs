macro_rules! cmd_modules {
    ( $( $name:ident ),* ) => {
        $(
            mod $name;
            pub use self::$name::$name;
        )*
    };
}

cmd_modules!(backup, restore, list, delete, unlock, rename);
