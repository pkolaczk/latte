use rand::rngs::SmallRng;
use rand::{Rng as RRng, SeedableRng};
use rune::runtime::{VmError, VmResult};
use rune::Value;
use std::any::Any;

#[derive(Debug, Clone, rune::Any)]
pub struct Rng(SmallRng);

impl Rng {
    pub fn with_seed(seed: i64) -> Self {
        Self::with_rng(SmallRng::seed_from_u64(seed as u64))
    }

    pub fn with_rng(rng: SmallRng) -> Self {
        Self(rng)
    }

    #[rune::function]
    pub fn gen_i64(&mut self) -> Value {
        Value::Integer(self.0.gen())
    }

    #[rune::function]
    pub fn gen_f64(&mut self) -> Value {
        Value::Float(self.0.gen())
    }

    #[rune::function]
    pub fn gen_range(&mut self, min: Value, max: Value) -> VmResult<Value> {
        match (min, max) {
            (Value::Integer(min), Value::Integer(max)) => {
                VmResult::Ok(Value::Integer(self.0.gen_range(min..max)))
            }
            (Value::Float(min), Value::Float(max)) => {
                VmResult::Ok(Value::Float(self.0.gen_range(min..max)))
            }
            (Value::Char(min), Value::Char(max)) => {
                VmResult::Ok(Value::Char(self.0.gen_range(min..max)))
            }
            (Value::Byte(min), Value::Byte(max)) => {
                VmResult::Ok(Value::Byte(self.0.gen_range(min..max)))
            }
            (min, max) => VmResult::Err(VmError::panic(format!(
                "Invalid argument types: {:?}, {:?}",
                min.type_id(),
                max.type_id()
            ))),
        }
    }
}
