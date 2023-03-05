package org.apache.iotdb.commons.pipe;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipePluginTable {

  private final Map<String, PipePluginInformation> pipePluginInformationMap;

  public PipePluginTable(){
    pipePluginInformationMap = new ConcurrentHashMap<>();
  }

  public void addPipePluginInformation(String pluginName, PipePluginInformation pipePluginInformation) {
    pipePluginInformationMap.put(pluginName.toUpperCase(), pipePluginInformation);
  }

  public void removePipePluginInformation(String pluginName) {
    pipePluginInformationMap.remove(pluginName.toUpperCase());
  }

  public PipePluginInformation getPipePluginInformation(String pluginName) {
    return pipePluginInformationMap.get(pluginName.toUpperCase());
  }

  public PipePluginInformation[] getAllPipePluginInformation() {
    return pipePluginInformationMap.values().toArray(new PipePluginInformation[0]);
  }

  public boolean containsPipePlugin(String pluginName) {
    return pipePluginInformationMap.containsKey(pluginName.toUpperCase());
  }

  public void serializePipePluginTable(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipePluginInformationMap.size(), outputStream);
    for (PipePluginInformation pipePluginInformation : pipePluginInformationMap.values()) {
      ReadWriteIOUtils.write(pipePluginInformation.serialize(), outputStream);
    }
  }

  public void deserializePipePluginTable(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      PipePluginInformation pipePluginInformation = PipePluginInformation.deserialize(inputStream);
      pipePluginInformationMap.put(pipePluginInformation.getPluginName().toUpperCase(), pipePluginInformation);
    }
  }

  public void clear() {
    pipePluginInformationMap.clear();
  }
}
