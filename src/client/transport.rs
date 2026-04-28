pub trait Io: std::io::Read + std::io::Write + Send {}
impl<T> Io for T where T: std::io::Read + std::io::Write + Send {}
pub type BoxedIo = Box<dyn Io>;
