package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LineCollectionEntry {
	ArrayList<String> lines_preloaded = null;
	BufferedReader lines_reader = null;

	String currentLine;
	File csv;

	public LineCollectionEntry(File csv, boolean preLoadLine) throws FileNotFoundException, IOException {
		this.csv = csv;
		if (preLoadLine) {
			lines_preloaded = new ArrayList<>(
					List.of(util.Util_7Z_CSV_Entry_Extract_Callable.extracted_lines_from_text(csv)));
		} else {
			lines_reader = new BufferedReader(new FileReader(csv));
		}
	}
	

	public boolean loadNextLine() {
		if (lines_preloaded != null) {
			if (lines_preloaded.size() > 0) {
				currentLine = lines_preloaded.remove(0);
			} else {
				currentLine = null;
			}
		} else {
			try {
				currentLine = lines_reader.readLine();
			} catch (IOException ex) {
				ex.printStackTrace(System.err);
				return false;
			}
		}
		return currentLine != null;
	}

	public String getCurrentLine() {
		return currentLine;
	}
	public File getCsv() {
		return csv;
	}

	public void closeReader() throws IOException {
		if (lines_reader != null) {
			lines_reader.close();
		}
	}

	

}