# To execute script launch this command on shell: hbase shell restore-snapshots.rb
# Run this is on source cluster

include Java
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.apache.hadoop.hbase.snapshot.ExportSnapshot
java_import org.apache.hadoop.util.ToolRunner

@conf = org.apache.hadoop.hbase.HBaseConfiguration.create

@admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(@conf)

# @return [Array]
def tables_list
  to_backup = Array.new
  tables = @admin.listTables('.*').to_a
  tables.each do |s|
    to_backup << s.getTableName().getNameAsString().to_s
  end
  to_backup
end

tables = tables_list
puts '-----------------------------------------------------'
puts 'Enabling replication for:'
puts tables
puts '-----------------------------------------------------'
puts 'Enabling replication.....'
tables.each { |t|
  puts "\tEnabling replication for table: #{t}"
  tableDescriptor = @admin.getTableDescriptor(t.to_java_bytes)
  @admin.disableTable(t)
  tableDescriptor.setValue('REPLICATION_SCOPE', '1')
  tableDescriptor.setValue('KEEP_DELETED_CELLS', 'true')

  columnDescriptors = tableDescriptor.getColumnFamilies()
  columnDescriptors.each { |ts|
    ts.setValue('REPLICATION_SCOPE', '1')
    ts.setValue('KEEP_DELETED_CELLS', 'true')
  }
  
  @admin.modifyTable(t, tableDescriptor)
  @admin.enableTable(t)
  puts "\tEnabled replication for table: #{t}"
}
puts '-----------------------------------------------------'
puts 'Done'
puts '-----------------------------------------------------'

exit 0