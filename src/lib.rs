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
    The 0 is preserved to be a seperator between the lengths and the data. This allows an unlimited
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
        if i.is_empty() {
            return Err("Input data contains an empty slice.".to_string());
        }
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

pub fn serialize_be(data: &[&[u8]]) -> Result<Vec<u8>, String> {
    if data.is_empty() {
        return Err("Input data is empty.".to_string());
    }

    let mut output = Vec::new();

    for i in data {
        if i.is_empty() {
            return Err("Input data contains an empty slice.".to_string());
        }
        if i.len() > (u64::MAX as usize) - 1 {
            return Err("Input data contains a slice that is too large to be serialized.".to_string());
        }
        output.extend_from_slice(&((1 + i.len()) as u64).to_be_bytes());
    }

    output.extend_from_slice(&[0; 8]);

    for i in data {
        output.extend_from_slice(i);
    }

    Ok(output)
}

pub fn deserialize_le(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut output = Vec::new();
    let mut lengths = Vec::new();

    let mut i = 0;

    while i < data.len() && data[i] != 0 {
        if data.len() - i < 1 {
            return Err("Input data is too short to deserialize.".to_string());
        }
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

    for j in lengths {
        output.push(data[i..(i + j)].to_vec());
        i += j;
    }

    Ok(output)
}

pub fn deserialize_be(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut output = Vec::new();
    let mut lengths = Vec::new();

    let mut i = 0;

    while i + 8 < data.len() && (data[i..(i + 8)] != [0; 8]) {
        if data.len() - i < 8 {
            return Err("Input data is too short to deserialize.".to_string());
        }
        let len = u64::from_be_bytes(data[i..(i + 8)].try_into().unwrap()) - 1;
        if len > u64::MAX - 1 {
            return Err("Input data contains a slice that is too large to be deserialized.".to_string());
        }
        lengths.push(len as usize);
        i += 8;
    }

    if i + 8 > data.len() {
        return Err("Input data is missing the terminating null bytes.".to_string());
    }

    i += 8;

    for j in lengths {
        if data.len() - i < j {
            return Err("Input data is too short to deserialize.".to_string());
        }
        output.push(data[i..(i + j)].to_vec());
        i += j;
    }

    Ok(output)
}
