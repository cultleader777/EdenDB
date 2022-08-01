#[macro_use]
extern crate lazy_static;

#[allow(dead_code, non_snake_case)]
mod database;

fn main() {
    let db = &database::DB;
    db.disks().rows_iter().filter(|i| {
        // we only care about intel disks
        db.disk_manufacturer().c_model(db.disks().c_make(*i)) == "intel"
    }).for_each(|intel_disk| {
        let parent = db.disks().c_parent(intel_disk);
        let children_disks = db.server().c_children_disks(parent);

        for pair in children_disks.windows(2) {
            match (
                db.disk_manufacturer().c_model(db.disks().c_make(pair[0])).as_str(),
                db.disk_manufacturer().c_model(db.disks().c_make(pair[1])).as_str(),
            ) {
                ("intel", "crucial") | ("crucial", "intel") => {
                    panic!("Are you kidding me bro?")
                }
                _ => {}
            }
        }
    });
}
