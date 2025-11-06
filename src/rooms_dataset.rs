use crate::types::Dataset;
use crate::types::Value;
use anyhow::{Context, anyhow};
use macros::Dataset;
use ordered_float::OrderedFloat;
use scraper::{Html, Selector};
use std::io::Read;

#[derive(Debug, Clone, Dataset)]
#[field_prefix("rooms_")]
pub struct Room {
    pub fullname: String,
    pub shortname: String,
    pub number: String,
    pub name: String,
    pub address: String,
    pub seats: OrderedFloat<f32>,
    pub r#type: String, // Renamed 'type' to 'r#type' as 'type' is a Rust keyword
    pub furniture: String,
    pub href: String,
}

/// Load rooms from a zip file containing HTML files
pub fn load_rooms_dataset(path_to_zip_file: &str) -> anyhow::Result<Vec<Room>> {
    let file = std::fs::File::open(path_to_zip_file)
        .with_context(|| format!("Failed to open zip file: {}", path_to_zip_file))?;

    let mut archive = zip::ZipArchive::new(file).context("Failed to read zip archive")?;

    // First, find and read index.html (could be index.htm)
    let index_content = read_index_from_archive(&mut archive)?;

    // Parse the index to get building information
    let building_entries = parse_index_for_buildings(&index_content)?;

    let mut all_rooms = Vec::new();

    // Process each building
    for (building_code, building_name, building_link) in building_entries {
        // Extract filename from the link
        let filename = extract_filename_from_link(&building_link);

        if !filename.is_empty() {
            // Try to find and read the building file from the archive
            if let Some(building_content) = read_file_from_archive(&mut archive, &filename) {
                // Pass the document for parsing the address
                let document = Html::parse_document(&building_content);
                let building_address = parse_building_address(&document);

                let rooms = parse_building_rooms(
                    &document,
                    &building_code,
                    &building_name,
                    &building_address,
                );
                all_rooms.extend(rooms);
            }
        }
    }

    Ok(all_rooms)
}

fn read_index_from_archive(archive: &mut zip::ZipArchive<std::fs::File>) -> anyhow::Result<String> {
    // Try both index.html and index.htm
    let index_names = ["index.html", "index.htm"];

    for index_name in &index_names {
        if let Ok(mut file) = archive.by_name(index_name) {
            let mut content = String::new();
            file.read_to_string(&mut content)
                .context("Failed to read index file content")?;
            return Ok(content);
        }
    }

    anyhow::bail!("Could not find index.html or index.htm in the archive")
}

fn read_file_from_archive(
    archive: &mut zip::ZipArchive<std::fs::File>,
    filename: &str,
) -> Option<String> {
    // Try to find the file (might be at root or in subdirectories)
    // First try exact filename
    if let Ok(mut file) = archive.by_name(filename) {
        let mut content = String::new();
        if file.read_to_string(&mut content).is_ok() {
            return Some(content);
        }
    }

    // Try to find by iterating through all files (in case it's in a subdirectory)
    for i in 0..archive.len() {
        if let Ok(mut file) = archive.by_index(i) {
            if file.name().ends_with(filename) {
                let mut content = String::new();
                if file.read_to_string(&mut content).is_ok() {
                    return Some(content);
                }
            }
        }
    }

    None
}

macro_rules! extract {
    ($elem:ident, $selector:ident) => {
        $elem
            .select(&$selector)
            .next()
            .and_then(|el| el.text().next())
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    };
}

fn parse_index_for_buildings(html_content: &str) -> anyhow::Result<Vec<(String, String, String)>> {
    let document = Html::parse_document(html_content);
    let mut buildings = Vec::new();

    // Selectors for the buildings table
    let row_selector = Selector::parse("table.views-table tbody tr")
        .map_err(|e| anyhow!("Failed to parse selector: {:?}", e))?;
    let code_selector = Selector::parse("td.views-field-field-building-code")
        .map_err(|e| anyhow!("Failed to parse selector: {:?}", e))?;
    let name_selector = Selector::parse("td.views-field-title a")
        .map_err(|e| anyhow!("Failed to parse selector: {:?}", e))?;

    for row in document.select(&row_selector) {
        let building_code = extract!(row, code_selector);

        let name_element = row.select(&name_selector).next();

        if let Some(name_el) = name_element {
            let building_name = name_el.text().collect::<String>().trim().to_string();

            let building_link = name_el.value().attr("href").unwrap_or("").to_string();

            buildings.push((building_code, building_name, building_link));
        }
    }

    Ok(buildings)
}

fn extract_filename_from_link(link: &str) -> String {
    // Links are in format: ./campus/discover/buildings-and-classrooms/BUILDING_CODE.htm
    link.rsplit('/').next().unwrap_or("").to_string()
}

// NEW FUNCTION to parse the building address
fn parse_building_address(document: &Html) -> String {
    let address_selector =
        Selector::parse("#building-info > div.building-field > div.field-content").unwrap(); // This selector targets the first div.building-field under #building-info

    // Find the address element and extract its text
    extract!(document, address_selector)
}

fn parse_building_rooms(
    document: &Html,
    building_code: &str,
    building_name: &str,
    building_address: &str, // Pass the extracted address
) -> Vec<Room> {
    let mut rooms = Vec::new();

    // Selectors for room table
    let room_row_selector = Selector::parse("table.views-table tbody tr").unwrap();
    let room_number_selector = Selector::parse("td.views-field-field-room-number a").unwrap();
    let capacity_selector = Selector::parse("td.views-field-field-room-capacity").unwrap();
    let furniture_selector = Selector::parse("td.views-field-field-room-furniture").unwrap();
    let room_type_selector = Selector::parse("td.views-field-field-room-type").unwrap();

    // Assuming the href comes from the room number link
    let href_selector = Selector::parse("td.views-field-field-room-number a").unwrap();

    for row in document.select(&room_row_selector) {
        // Extract fullname (assuming it's the building_name for each room for now)
        let fullname = building_name.to_string();

        // Extract shortname (assuming it's the building_code for each room for now)
        let shortname = building_code.to_string();

        // Extract room number
        let number = extract!(row, room_number_selector);

        // Construct name as "rooms_shortname"_"rooms_number"
        let name = format!("{}_{}", shortname, number);

        // Use the building_address passed into the function
        let address = building_address.to_string();

        // Extract seats (capacity)
        let seats = row
            .select(&capacity_selector)
            .next()
            .and_then(|el| el.text().next())
            .and_then(|s| s.trim().parse::<f32>().ok())
            .map(OrderedFloat)
            .unwrap_or(OrderedFloat::from(0.0f32));

        // Extract type
        let room_type = extract!(row, room_type_selector);

        // Extract furniture
        let furniture = extract!(row, furniture_selector);

        // Extract href (assuming it's the link from the room number)
        let href = row
            .select(&href_selector)
            .next()
            .and_then(|el| el.value().attr("href"))
            .map(|s| s.to_string())
            .unwrap_or_default();

        rooms.push(Room {
            fullname,
            shortname,
            number,
            name,
            address,
            seats,
            r#type: room_type, // Using 'r#type' for the keyword 'type'
            furniture,
            href,
        });
    }

    rooms
}
