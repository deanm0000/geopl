mod kmz;
use kmz::read_kml;
use std::env;


fn main() {
    let mut args: Vec<String> = env::args().collect();

    let (source, sink) = match args.len() {
        2=> (args.remove(1), None),
        3=> {
            let sink = args.remove(2);
            let source = args.remove(1);
            (source, Some(sink))
        },
        _=> panic!("unsupported args")
    };
    let df = read_kml(
        source, sink
    );
    eprintln!("{}", df);
}
