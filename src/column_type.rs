use std::fmt::Display;
use std::hash::Hash;

pub trait ColumnType: Hash + Display {}

pub struct TextColumnType;

pub struct IntColumnType;

pub struct FloatColumnType;

pub struct RealColumnType;
