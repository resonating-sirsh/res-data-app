INSERT_MESSAGE = """
mutation insertNotification($record_value: jsonb) {
  insert_infraestructure_notification_dump(objects: {record_value: $record_value}) {
    affected_rows
    returning {
      id
      record_value
      sent_at
      created_at
    }
  }
}
"""
