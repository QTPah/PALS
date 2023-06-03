/*      PALS (Pre-Allocated Length Serialization)
    This is a serialization format that is designed to be fast and efficient.
    It works by pre-allocating the length of the data to be serialized, and
    then serializing the data. This allows for the data to be deserialized
    without having to read the length of the data first, which is useful for
    when you are reading from a stream of data, such as a file or a network

    The whole format consists of 2 parts:
        1. The length of each data segment
        2. The data segments itself

    The fitst part is a list of 64-bit unsigned integers, each shifted by 1.
    The 0 is preserved to be a seperator between the lengths and the data. This allows an unlimited
    number of data segments to be serialized, with each segment having a
    maximum length of 2^62 bytes (4 exabytes).
*/

use std::convert::TryInto;

pub fn serialize_le(data : &[&[u8]]) -> Vec<u8> {
    let mut output = Vec::new();

    for i in data {
        output.extend_from_slice(&((1 + i.len()) as u8).to_le_bytes());
    }
    output.push(0);

    for i in data {
        output.extend_from_slice(i);
    }

    output
}

pub fn serialize_be(data : &[&[u8]]) -> Vec<u8> {
    let mut output = Vec::new();

    for i in data {
        output.extend_from_slice(&((1 + i.len()) as u64).to_be_bytes());
    }
    output.extend_from_slice(&[0; 8]);

    for i in data {
        output.extend_from_slice(i);
    }

    output
}

pub fn deserialize_le(data: &[u8]) -> Vec<Vec<u8>> {
    let mut output = Vec::new();
    let mut lengths = Vec::new();

    let mut i = 0;

    while data[i] != 0 {
        lengths.push(data[i] - 1 as u8);
        i += 1;
    }

    i += 1;

    for j in lengths {
        output.push(data[i..(i + j as usize)].to_vec());
        i += j as usize;
    }

    output
}

pub fn deserialize_be(data: &[u8]) -> Vec<Vec<u8>> {
    let mut output = Vec::new();
    let mut lengths = Vec::new();

    let mut i = 0;

    while i + 8 < data.len() && (data[i..(i + 8)] != [0; 8]) {
        lengths.push(u64::from_be_bytes(data[i..(i + 8)].try_into().unwrap()) - 1);
        println!("{}", u64::from_be_bytes(data[i..(i + 8)].try_into().unwrap()) - 1);
        i += 8;
    }

    i += 8;

    for j in lengths {
        println!("{} {}", j, i);
        if i + j as usize > data.len() {
            break;
        }
        output.push(data[i..(i + j as usize)].to_vec());
        i += j as usize;
    }

    output
}
