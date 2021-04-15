# To execute script launch this command on shell: hbase shell snapshot-tables.rb
@peer_culster_addr = "address of peer cluster"

@snapshot_list = Array.new
@copy_table_list = Array.new

include Java
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.HBaseAdmin

@conf = org.apache.hadoop.hbase.HBaseConfiguration.create

@admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(@conf)

def cleanup
  puts 'Deleting all old snapshots.....'
  old_snapshots = @admin.listSnapshots(".*-SNAPSHOT-.*").to_a
  old_snapshots.each do |s|
    puts "\tDeleting snapshot: #{s.name}"
    @admin.deleteSnapshot(s.name.to_java_bytes)
  end
  puts 'Done deleting all old snapshots.'
end

# @return [Array]
def tables_to_backup
  to_backup = Array.new
  tables = @admin.listTables(".*").to_a
  tables.each do |s|
    to_backup << s.getTableName().getNameAsString().to_s
  end
  to_backup
end

def backup(table)
  cur_time = Time.now
  snapshot_name = "#{table}-SNAPSHOT-#{cur_time.strftime("%Y%m%d_%H%M%S")}"
  puts "\tCreating Snapshot: #{snapshot_name}"
  @admin.snapshot(snapshot_name.to_java_bytes, table.to_java_bytes, HBaseProtos.SnapshotDescription.Type.FLUSH)
  @snapshot_list << snapshot_name
  @copy_table_list << "sudo -u hbase hbase org.apache.hadoop.hbase.mapreduce.CopyTable -Dhbase.meta.replicas.use=true --starttime=#{cur_time.to_i * 1000} --peer.adr=#{@peer_culster_addr} #{table}"
end

tables = tables_to_backup()
puts 'Tables to backup:'
puts tables
puts 'Starting backup.....'

tables.each do |t|
  backup(t)
end

File.open("snapshots-list.txt", "w+") do |f|
  @snapshot_list.each { |element| f.puts(element) }
end

File.open("copy-tables.sh", "w+") do |f|
  f.puts('#!/bin/bash')
  @copy_table_list.each { |element| f.puts(element) }
end

puts 'Done'

exit 0
