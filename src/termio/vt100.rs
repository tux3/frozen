use std::fmt::{self, Display};

pub enum VT100 {
    ClearLine,
    InsertAbove,
    RemoveLine,
    MoveUp(usize),
    MoveDown(usize),
    StyleReset,
    StyleError,
    StyleWarning,
    StyleActive,
}

impl Display for VT100 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            VT100::ClearLine => f.write_str("\x1B[2K\r"),
            VT100::InsertAbove => f.write_str("\x1BM\x1B[B\x1B[1L"),
            VT100::RemoveLine => f.write_str("\x1B[1M"),
            VT100::MoveUp(0) | VT100::MoveDown(0) => Ok(()),
            VT100::MoveUp(n) => write!(f, "\x1B[{}A", n),
            VT100::MoveDown(n) => write!(f, "\x1B[{}B", n),
            VT100::StyleReset => f.write_str("\x1B[0m"),
            VT100::StyleError => f.write_str("\x1B[31m"),
            VT100::StyleWarning => f.write_str("\x1B[33m"),
            VT100::StyleActive => f.write_str("\x1B[1m"),
        }
    }
}

pub fn remove_at(offset: usize) {
    print!("{}{}{}\r", VT100::MoveUp(offset), VT100::RemoveLine, VT100::MoveDown(offset-1));
}

pub fn insert_at(offset: usize, style: VT100, str: &str) {
    println!("{}{}\r{}{}{}{}", VT100::MoveUp(offset), VT100::InsertAbove, style, str,
                                VT100::StyleReset, VT100::MoveDown(offset));
}

pub fn rewrite_at(offset: usize, style: VT100, str: &str) {
    print!("{}{}\r{}{}{}{}", VT100::MoveUp(offset), VT100::ClearLine, style, str,
                                VT100::StyleReset, VT100::MoveDown(offset));
}

pub fn write_at(offset: usize, style: VT100, str: &str) {
    print!("{}\r{}{}{}{}", VT100::MoveUp(offset), style, str,
                            VT100::StyleReset, VT100::MoveDown(offset));
}