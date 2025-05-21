use std::io::{self, BufRead as _, Write as _};

#[no_mangle]
pub fn main() -> Result<(), std::io::Error> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    let stdin = io::BufReader::new(stdin.lock());
    let mut stdout = io::BufWriter::new(stdout.lock());

    for line in stdin.lines() {
        let Ok(line) = line else {
            write!(&mut stdout, "Error:IO\n")?;
            break;
        };

        let Ok(num) = line.parse::<u64>() else {
            write!(&mut stdout, "Error:NUM\n")?;
            continue;
        };

        if num % 15 == 0 {
            write!(&mut stdout, "FizzBuzz\n")?;
        } else if num % 5 == 0 {
            write!(&mut stdout, "Buzz\n")?;
        } else if num % 3 == 0 {
            write!(&mut stdout, "Fizz\n")?;
        } else {
            write!(&mut stdout, "{}\n", num)?;
        }
    }

    Ok(())
}
