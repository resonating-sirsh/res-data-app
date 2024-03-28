INSERT_MESSAGE = """
mutation InsertNotifications($destination: String, $message: String, $source_table_id: String, $channels: json, $subchannel: String, $links: json, $topic: String, $read_at: timestamptz, $read: Boolean, $received: Boolean, $should_send: Boolean, $source_record_id: String) {
  insert_infraestructure_res_notifications(objects: {destination: $destination, message: $message, source_table_id: $source_table_id, channels: $channels, subchannel: $subchannel, links: $links, topic: $topic, read_at: $read_at, read: $read, received: $received, should_send: $should_send, source_record_id: $source_record_id}) {
    affected_rows
  }
}
"""

GET_MESSAGE = """
query GetNotifications {
  infraestructure_res_notifications {
    source_record_id
    source_table_id
    should_send
    id
  }
}
"""

UPDATE_MESSAGE = """
mutation UpdateNotification($destination: String, $message: String, $source_table_id: String, $channels: json, $subchannel: String, $links: json, $topic: String, $read_at: timestamptz, $read: Boolean, $received: Boolean, $should_send: Boolean, $source_record_id: String) {
  update_infraestructure_res_notifications(where: {source_record_id: {_eq: $source_record_id}}, _set: {destination: $destination, message: $message, source_table_id: $source_table_id, channels: $channels, subchannel: $subchannel, links: $links, topic: $topic, read_at: $read_at, read: $read, received: $received, should_send: $should_send, source_record_id: $source_record_id}) {
    affected_rows
  }
}
"""
