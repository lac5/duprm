type Byte = u8;
type Buffer = Vec<Byte>;

pub fn remove_tags_from_buffer(data: Buffer) -> Option<Buffer> {
    let frame_position = match get_frame_position(&data) {
        Some(x) => x,
        None => return Some(data),
    };

    let h_size: Buffer = vec![
        data[frame_position + 6],
        data[frame_position + 7],
        data[frame_position + 8],
        data[frame_position + 9],
    ];

    if (h_size[0] | h_size[1] | h_size[2] | h_size[3]) & 0x80 != 0 {
        return None;
    }

    let len = data.len();
    if len >= frame_position + 10 {
        let size = decode_size(&data[(frame_position + 6)..(frame_position + 10)]);
        if len >= frame_position + size + 10 {
            let mut ret1 = data[0..frame_position].to_vec();
            let mut ret2 = data[(frame_position + size + 10)..].to_vec();
            ret1.append(&mut ret2);
            return Some(ret1);
        }
    }

    Some(data)
}

fn read_u24_be(buffer: &[Byte], offset: usize) -> Option<u32> {
    let len = buffer.len();
    if len - offset < 3 {
        return None;
    }
    let mut sum: u32 = 0;
    sum |= u32::from(buffer[offset + 0]) << 16;
    sum |= u32::from(buffer[offset + 1]) << 8;
    sum |= u32::from(buffer[offset + 2]) << 0;
    Some(sum)
}

fn is_valid_id3_header(buffer: &[Byte]) -> bool {
    !(read_u24_be(&buffer, 0).iter().any(|&x| x != 0x494433)
        || !(buffer[3] == 0x02 || buffer[3] == 0x03 || buffer[3] == 0x04)
        || buffer[4] != 0x00
        || buffer[6] & 128 == 1
        || buffer[7] & 128 == 1
        || buffer[8] & 128 == 1
        || buffer[9] & 128 == 1)
}

fn get_frame_position(buffer: &Buffer) -> Option<usize> {
    let mut frame_position: Option<usize>;
    let mut next_position = 0;
    let mut frame_header_valid = false;
    let len = buffer.len();
    loop {
        frame_position =
            if let Some(x) = buffer[next_position..].windows(3).position(|r| r == b"ID3") {
                let x = next_position + x;
                if x + 10 < len {
                    let slice = &buffer[x..(x + 10)];
                    frame_header_valid = is_valid_id3_header(slice);
                    next_position = x + 3;
                    Some(x)
                } else {
                    None
                }
            } else {
                None
            };
        if frame_position == None || frame_header_valid {
            break;
        }
    }

    if !frame_header_valid {
        None
    } else {
        frame_position
    }
}

fn decode_size(h_size: &[Byte]) -> usize {
    let h_size0 = h_size[0] as usize;
    let h_size1 = h_size[1] as usize;
    let h_size2 = h_size[2] as usize;
    let h_size3 = h_size[3] as usize;
    (h_size0 << 21) + (h_size1 << 14) + (h_size2 << 7) + h_size3
}
