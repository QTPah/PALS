/*      PALS (Pre-Allocated Length Serialization)
    This is a serialization format that is designed to be fast and efficient.
    It works by pre-allocating the length of the data to be serialized, and
    then serializing the data. This allows for the data to be deserialized
    without having to read the length of the data first, which is useful for
    when you are reading from a stream of data, such as a file or a network

    The whole format consists of 2 parts:
        1. The length of each data segment
        2. The data segments itself

    The first part is a list of 64-bit unsigned integers, each shifted by 1.
    The 0 is preserved to be a separator between the lengths and the data. This allows an unlimited
    number of data segments to be serialized, with each segment having a
    maximum length of 2^62 bytes (4 exabytes).
*/

use std::convert::TryInto;

pub fn serialize_le(data: &[&[u8]]) -> Result<Vec<u8>, String> {
    if data.is_empty() {
        return Err("Input data is empty.".to_string());
    }

    let mut output = Vec::new();

    for i in data {
        if i.len() > (u8::MAX as usize) - 1 {
            return Err("Input data contains a slice that is too large to be serialized.".to_string());
        }
        output.extend_from_slice(&((1 + i.len()) as u8).to_le_bytes());
    }

    output.push(0);

    for i in data {
        output.extend_from_slice(i);
    }

    Ok(output)
}

// Serialize a vector of byte vectors into a single byte slice.
// Each byte vector represents a slice of the output data.
pub fn serialize_be(data: &[&[u8]]) -> Result<Vec<u8>, String> {
    if data.is_empty() {
        return Err("Input data is empty.".to_string());
    }

    let mut output = Vec::new(); // Initialize an empty vector to hold the output data.

    // Loop through the input data and add the length of each slice to the output vector.
    for i in data {
        output.extend_from_slice(&((1 + i.len()) as u64).to_be_bytes()); // Add the length to the output vector.
    }

    output.extend_from_slice(&[0; 8]); // Add a zero-length slice to the output vector.

    // Loop through the input data and add each slice to the output vector.
    for i in data {
        output.extend_from_slice(i); // Add the slice to the output vector.
    }

    Ok(output) // Return the output vector.
}

pub fn deserialize_le(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut output = Vec::new();
    let mut lengths = Vec::new();

    let mut i = 0;

    while i < data.len() && data[i] != 0 {
        let len = data[i] as usize;
        if len > data.len() - i - 1 {
            return Err("Input data contains a slice that is too large to be deserialized.".to_string());
        }
        lengths.push(len - 1);
        i += 1;
    }

    if i == data.len() {
        return Err("Input data is missing the terminating null byte.".to_string());
    }

    i += 1;

    if i + lengths.iter().sum::<usize>() > data.len() {
        return Err("Input data is incomplete.".to_string());
    }

    for j in lengths {
        output.push(data[i..(i + j)].to_vec());
        i += j;
    }

    Ok(output)
}

// Deserialize a byte slice into a vector of byte vectors.
// Each byte vector represents a slice of the input data.
pub fn deserialize_be(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut output = Vec::new(); // Initialize an empty vector to hold the output byte vectors.
    let mut lengths = Vec::new(); // Initialize an empty vector to hold the lengths of the input slices.

    let mut i = 0; // Initialize a counter variable to keep track of the current position in the input data.

    // Loop through the input data until we reach the end or encounter a zero-length slice.
    while i + 8 <= data.len() && (data[i..(i + 8)] != [0; 8]) {
        // Read the length of the next slice from the input data.
        let len = u64::from_be_bytes(data[i..(i + 8)].try_into().unwrap()) - 1;

        // Check if the length is too large to be deserialized.
        if len > u64::MAX - 1 {
            return Err("Input data contains a slice that is too large to be deserialized.".to_string());
        }

        // Add the length to the lengths vector.
        lengths.push(len as usize);

        // Move the counter variable to the start of the next slice.
        i += 8;
    }

    i += 8; // Move the counter variable past the zero-length slice.

    // Check if the combined lengths of the slices is greater than the length of the input data.
    if i + lengths.iter().sum::<usize>() > data.len() {
        return Err("Input data is incomplete.".to_string());
    }

    // Loop through the lengths vector and extract the corresponding slices from the input data.
    for j in lengths {
        output.push(data[i..(i + j)].to_vec()); // Add the slice to the output vector.
        i += j; // Move the counter variable to the start of the next slice.
    }

    Ok(output) // Return the output vector.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_be() {
        let data = vec![
            vec![1, 2, 3],
            vec![4, 5, 6, 7],
            vec![],
            vec![8, 9],
            vec![10, 11, 12, 13, 14],
        ];

        let serialized = serialize_be(&data.iter().map(|v| v.as_slice()).collect::<Vec<_>>()).unwrap();
        let deserialized = deserialize_be(&serialized).unwrap();

        assert_eq!(data, deserialized);
    }

    #[test]
    fn test_serialize_be_empty_input() {
        let data = Vec::<Vec<u8>>::new();

        let result = serialize_be(&data.iter().map(|v| v.as_slice()).collect::<Vec<_>>());

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Input data is empty.");
    }

    #[test]
    fn test_deserialize_be_incomplete_input() {
        let data = vec![0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3];

        let result = deserialize_be(&data);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Input data is incomplete.");
    }



    #[test]
    fn test_serialize_deserialize_le() {
        let data = vec![
            vec![1, 2, 3],
            vec![4, 5, 6, 7],
            vec![8, 9],
            vec![],
            vec![10, 11, 12, 13, 14],
        ];

        let serialized = serialize_le(&data.iter().map(|v| v.as_slice()).collect::<Vec<_>>()).unwrap();
        let deserialized = deserialize_le(&serialized).unwrap();

        assert_eq!(data, deserialized);
    }

    #[test]
    fn test_serialize_le_empty_input() {
        let data = Vec::<Vec<u8>>::new();

        let result = serialize_le(&data.iter().map(|v| v.as_slice()).collect::<Vec<_>>());

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Input data is empty.");
    }

    #[test]
    fn test_serialize_le_large_input() {
        let data = vec![vec![0; 1_000_000_000]];

        let result = serialize_le(&data.iter().map(|v| v.as_slice()).collect::<Vec<_>>());

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Input data contains a slice that is too large to be serialized.");
    }

    #[test]
    fn test_deserialize_le_incomplete_input() {
        let data = vec![
            vec![1, 2, 3],
            vec![4, 5, 6, 7],
            vec![8, 9],
            vec![],
            vec![10, 11, 12, 13, 14],
        ];

        let serialized = serialize_le(&data.iter().map(|v| v.as_slice()).collect::<Vec<_>>()).unwrap();

        let result = deserialize_le(&serialized[0..(serialized.len() - 1)]);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Input data is incomplete.");
    }
}