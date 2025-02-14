/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use dbschema::{
    DateTimeSchema, DbSchema, DictionarySchema, EnumSchema, IntegerSchema,
    Ipv4Schema, Ipv6Schema, ListSchema, OptionSchema, SetSchema, StringSchema,
    StructSchema, UnitSchema,
};

fn main() {
    let schema: DbSchema = StructSchema::new()
        .field("host_id", StringSchema::new())
        .field("now", DateTimeSchema::new())
        .field(
            "snmp",
            StructSchema::new()
                .field(
                    "snmpV2Mib",
                    StructSchema::new()
                        .field("sysName", StringSchema::new())
                        .field(
                            "sysVendor",
                            OptionSchema::new(StringSchema::new()),
                        )
                        .field(
                            "sysType",
                            EnumSchema::new()
                                .default("switch")
                                .option("switch", UnitSchema::new())
                                .option("router", UnitSchema::new()),
                        ),
                )
                .field(
                    "ipForwardingMib",
                    StructSchema::new().field(
                        "routeTable",
                        DictionarySchema::new(
                            StructSchema::new()
                                .field("name", StringSchema::new())
                                .field("gateway", StringSchema::new())
                                .field(
                                    "type",
                                    EnumSchema::new()
                                        .default("fixed")
                                        .option("fixed", UnitSchema::new())
                                        .option("dynamic", UnitSchema::new()),
                                ),
                        ),
                    ),
                )
                .field(
                    "addresses",
                    SetSchema::new(
                        EnumSchema::new()
                            .option("ipv4", Ipv4Schema::new())
                            .option("ipv6", Ipv6Schema::new()),
                    ),
                ),
        )
        .field("numbers", ListSchema::new(IntegerSchema::new()))
        .into();

    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
