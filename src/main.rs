use polars::prelude::LazyCsvReader;
use core::str;
use std::io::{Read, Seek, Write, BufReader};
use std::fs::File;
use std::path::Path;
use flate2::bufread::GzDecoder;

fn main() -> std::io::Result<()> {

    let mut file = File::open("./data/test.csv.gz")?;
    let mut reader = BufReader::new(file);

    // data_root.join(path)
    let data_root = Path::new("./data/");
    let mut gz = GzDecoder::new(reader);

    let mut buffer = [0;2048];
    
    let mut bytes_read = 0;
    println!("Enter while loop.");
    while let Ok(n) = gz.read(&mut buffer) {

        let sep = b'\n';
        let lines = buffer.split(|s| *s == sep);
        for (i, line) in lines.into_iter().enumerate() {
            match str::from_utf8(line) {
                Ok(v) => {
                    println!("Line {}", i);
                    println!("{}", v);
                },
                _ => {println!("Cannot turn bytes to string.")}
            }
        }
        bytes_read += n;
        break
    }
    println!("Finished while loop.");

    // let mut i:usize = 0; 
    // println!("Before while loop.");
    // while let Ok(Some(mut z)) = read_zipfile_from_stream(&mut reader) {
    //     let name = format!("{}_{}.csv", z.name(), i.to_string());
    //     let new_path = data_root.join(name);
    //     let mut out_file = File::create(new_path.as_path()).unwrap();
    //     let mut buf = vec![];
    //     println!("{}", z.compressed_size());
    //     match z.read_to_end(&mut buf) {
    //         Ok(_) => {},
    //         Err(e) => return Err(e),
    //     }
    //     // let _ = z.read_to_end(&mut buf);
    //     out_file.write_all(&buf).unwrap();
    //     i += 1;
    // }
    // println!("After while loop.");
    Ok(())
}
