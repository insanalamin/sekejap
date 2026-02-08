//! Vector Quantization - FP32 → FP16/INT8 for memory reduction
//!
//! This module provides per-vector quantization to reduce memory footprint.
//!
//! # Quantization Types
//!
//! - **None**: FP32 (4 bytes per dimension) - No quantization
//! - **FP16**: 2 bytes per dimension - 50% reduction
//! - **INT8**: 1 byte per dimension - 75% reduction
//!
//! # Per-Vector Scaling
//!
//! Each vector maintains its own min/max scale to prevent "context poisoning"
//! where a single outlier ruins precision for the entire dataset.
//!
//! # Example
//!
//! ```rust
//! use sekejap::vectors::quantization::{quantize, dequantize, QuantizationType};
//!
//! # fn main() {
//! let vec = vec![0.1, 0.2, 0.3, 0.4];
//! let quantized = quantize(&vec, QuantizationType::INT8);
//! let decoded = dequantize(&quantized);
//! # }
//! ```

use serde::{Deserialize, Serialize};

/// Quantization type for vector storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantizationType {
    /// No quantization - FP32 (4 bytes per dimension)
    None,
    /// Half-precision floating point - 2 bytes per dimension
    FP16,
    /// 8-bit integer - 1 byte per dimension
    INT8,
}

impl Default for QuantizationType {
    fn default() -> Self {
        QuantizationType::None
    }
}

/// Quantized vector with per-vector scaling
///
/// Stores vector in compressed format with min/max scale information
/// for accurate dequantization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizedVector {
    /// Quantized data (INT8 or FP16 depending on type)
    pub data: Vec<u8>,
    /// Min value in original vector (for scaling)
    pub min: f32,
    /// Max value in original vector (for scaling)
    pub max: f32,
    /// Quantization type used
    pub quant_type: QuantizationType,
}

impl QuantizedVector {
    /// Create a new quantized vector
    pub fn new(data: Vec<u8>, min: f32, max: f32, quant_type: QuantizationType) -> Self {
        Self {
            data,
            min,
            max,
            quant_type,
        }
    }

    /// Get number of dimensions in this vector
    pub fn dim(&self) -> usize {
        match self.quant_type {
            QuantizationType::None => self.data.len() / 4,
            QuantizationType::FP16 => self.data.len() / 2,
            QuantizationType::INT8 => self.data.len(),
        }
    }
}

/// Quantize a vector using specified type
///
/// # Arguments
///
/// * `vec` - Original FP32 vector
/// * `quant_type` - Quantization type to use
///
/// # Returns
///
/// Quantized vector with per-vector scaling information
///
/// # Example
///
/// ```rust
/// # use sekejap::vectors::quantization::{quantize, QuantizationType};
/// let vec = vec![0.1, 0.2, 0.3, 0.4];
/// let quantized = quantize(&vec, QuantizationType::INT8);
/// ```
pub fn quantize(vec: &[f32], quant_type: QuantizationType) -> QuantizedVector {
    match quant_type {
        QuantizationType::None => {
            // No quantization - just copy as bytes
            let data = super::ops::vector_to_bytes(vec);
            QuantizedVector::new(data, 0.0, 1.0, QuantizationType::None)
        }
        QuantizationType::FP16 => {
            // Quantize to FP16
            quantize_fp16(vec)
        }
        QuantizationType::INT8 => {
            // Quantize to INT8
            quantize_int8(vec)
        }
    }
}

/// Quantize vector to FP16 (half-precision)
///
/// Uses half crate for FP16 conversion
fn quantize_fp16(vec: &[f32]) -> QuantizedVector {
    let min = vec.iter().fold(f32::INFINITY, |a, &b| a.min(b));
    let max = vec.iter().fold(f32::NEG_INFINITY, |a, &b| a.max(b));

    let mut data = Vec::with_capacity(vec.len() * 2);
    for &val in vec {
        // Use half crate to convert f32 to f16
        #[cfg(feature = "vector")]
        {
            use half::f16;
            let f16 = f16::from_f32(val);
            data.extend_from_slice(&f16.to_le_bytes());
        }
        #[cfg(not(feature = "vector"))]
        {
            // Fallback: just cast to u16 (not accurate but prevents compilation error)
            let truncated = if val > 0.0 { val as u16 } else { 0 };
            data.extend_from_slice(&truncated.to_le_bytes());
        }
    }

    QuantizedVector::new(data, min, max, QuantizationType::FP16)
}

/// Quantize vector to INT8 with per-vector scaling
///
/// Maps values to [0, 255] using min/max scaling
fn quantize_int8(vec: &[f32]) -> QuantizedVector {
    let min = vec.iter().fold(f32::INFINITY, |a, &b| a.min(b));
    let max = vec.iter().fold(f32::NEG_INFINITY, |a, &b| a.max(b));

    let range = max - min;
    let data: Vec<u8> = if range == 0.0 {
        // All values are the same
        vec.iter().map(|_| 127u8).collect()
    } else {
        vec.iter()
            .map(|&val| {
                let normalized = (val - min) / range;
                (normalized * 255.0).round() as u8
            })
            .collect()
    };

    QuantizedVector::new(data, min, max, QuantizationType::INT8)
}

/// Dequantize vector back to FP32
///
/// # Arguments
///
/// * `quantized` - Quantized vector
///
/// # Returns
///
/// Reconstructed FP32 vector (with quantization error)
///
/// # Example
///
/// ```rust
/// # use sekejap::vectors::quantization::{quantize, dequantize, QuantizationType};
/// let vec = vec![0.1, 0.2, 0.3, 0.4];
/// let quantized = quantize(&vec, QuantizationType::INT8);
/// let decoded = dequantize(&quantized);
/// ```
pub fn dequantize(quantized: &QuantizedVector) -> Vec<f32> {
    match quantized.quant_type {
        QuantizationType::None => {
            // No quantization - just parse bytes back
            super::ops::bytes_to_vector(&quantized.data)
        }
        QuantizationType::FP16 => {
            // Dequantize from FP16
            dequantize_fp16(quantized)
        }
        QuantizationType::INT8 => {
            // Dequantize from INT8
            dequantize_int8(quantized)
        }
    }
}

/// Dequantize FP16 vector back to FP32
fn dequantize_fp16(quantized: &QuantizedVector) -> Vec<f32> {
    let mut vec = Vec::with_capacity(quantized.data.len() / 2);

    for chunk in quantized.data.chunks_exact(2) {
        #[cfg(feature = "vector")]
        {
            use half::f16;
            let bytes = [chunk[0], chunk[1]];
            let f16 = f16::from_le_bytes(bytes);
            vec.push(f16.to_f32());
        }
        #[cfg(not(feature = "vector"))]
        {
            // Fallback: just interpret as u16
            let bytes = [chunk[0], chunk[1]];
            let val = u16::from_le_bytes(bytes) as f32;
            vec.push(val);
        }
    }

    vec
}

/// Dequantize INT8 vector back to FP32
fn dequantize_int8(quantized: &QuantizedVector) -> Vec<f32> {
    let range = quantized.max - quantized.min;

    if range == 0.0 {
        // All values were the same
        vec![quantized.min; quantized.data.len()]
    } else {
        quantized
            .data
            .iter()
            .map(|&val| {
                let normalized = val as f32 / 255.0;
                quantized.min + normalized * range
            })
            .collect()
    }
}

/// Compute quantization error (mean absolute error)
///
/// # Arguments
///
/// * `original` - Original FP32 vector
/// * `quantized` - Quantized vector
///
/// # Returns
///
/// Mean absolute error between original and dequantized vector
///
/// # Example
///
/// ```rust
/// # use sekejap::vectors::quantization::{quantize, quantization_error, QuantizationType};
/// let vec = vec![0.1, 0.2, 0.3, 0.4];
/// let quantized = quantize(&vec, QuantizationType::INT8);
/// let error = quantization_error(&vec, &quantized);
/// println!("Quantization MAE: {:.6}", error);
/// ```
pub fn quantization_error(original: &[f32], quantized: &QuantizedVector) -> f32 {
    let decoded = dequantize(quantized);

    if decoded.len() != original.len() {
        return f32::NAN;
    }

    let error: f32 = original
        .iter()
        .zip(decoded.iter())
        .map(|(&orig, &dec)| (orig - dec).abs())
        .sum();

    error / original.len() as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantize_none() {
        let vec = vec![0.1, 0.2, 0.3, 0.4];
        let quantized = quantize(&vec, QuantizationType::None);
        assert_eq!(quantized.quant_type, QuantizationType::None);

        let decoded = dequantize(&quantized);
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_quantize_int8() {
        let vec = vec![0.0, 0.5, 1.0];
        let quantized = quantize(&vec, QuantizationType::INT8);
        assert_eq!(quantized.quant_type, QuantizationType::INT8);
        assert_eq!(quantized.min, 0.0);
        assert_eq!(quantized.max, 1.0);
    }

    #[test]
    fn test_dequantize_int8() {
        let vec = vec![0.0, 0.5, 1.0];
        let quantized = quantize(&vec, QuantizationType::INT8);
        let decoded = dequantize(&quantized);

        assert_eq!(decoded.len(), vec.len());
        // Small quantization error expected
        for (orig, dec) in vec.iter().zip(decoded.iter()) {
            assert!((orig - dec).abs() < 0.01);
        }
    }

    #[test]
    fn test_quantization_error() {
        let vec = vec![0.0, 0.5, 1.0];
        let quantized = quantize(&vec, QuantizationType::INT8);
        let error = quantization_error(&vec, &quantized);

        // Error should be small for INT8
        assert!(error < 0.01);
    }

    #[test]
    fn test_quantized_vector_dim() {
        let vec = vec![0.1; 100];
        let quantized = quantize(&vec, QuantizationType::INT8);

        assert_eq!(quantized.dim(), 100);
        assert_eq!(quantized.data.len(), 100); // 1 byte per dimension
    }

    #[test]
    fn test_fp16_dim() {
        let vec = vec![0.1; 50];
        let quantized = quantize(&vec, QuantizationType::FP16);

        assert_eq!(quantized.dim(), 50);
        assert_eq!(quantized.data.len(), 100); // 2 bytes per dimension
    }
}
