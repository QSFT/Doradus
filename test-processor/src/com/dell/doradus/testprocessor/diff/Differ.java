/*
 * Copyright (C) 2014 Dell, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.testprocessor.diff;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.dell.doradus.testprocessor.common.Utils;

public class Differ
{
    private boolean m_ignoreWhiteSpace;
    //TODO: private boolean m_ignoreEmptyLines;
    private boolean m_ignoreCase;

    public Differ(boolean ignoreWhiteSpace, boolean ignoreCase) {
    	m_ignoreWhiteSpace = ignoreWhiteSpace;
    	m_ignoreCase = ignoreCase;
    }
    
    public CompareResult compareFiles(String pathA, String pathB)
    throws Exception
    {
    	String textA = readTextFile(pathA);
    	String textB = readTextFile(pathB);
    	
    	return compareStrings(textA, textB);
    }
    
    private String readTextFile(String path)
    throws Exception
    {
    	return new String(Files.readAllBytes(Paths.get(path)));
    }
    
    public CompareResult compareStrings(String textA, String textB)
    throws Exception
    {
    	List<DiffLine> diffLines = new ArrayList<DiffLine>();
        boolean identical = true;

        DiffResult diffResult = createLineDiffs(textA, textB);
        int posB = 0;
        
        for (int ind = 0; ind < diffResult.diffBlocks.size(); ind++)
        {
        	DiffBlock diffBlock = diffResult.diffBlocks.get(ind);
        	
            for (; posB < diffBlock.insertStartB; posB++) {
                diffLines.add(new DiffLine(ChangeType.UNCHANGED, diffResult.linesB[posB]));
            }

            int i = 0;
            for (; i < Math.min(diffBlock.deleteCountA, diffBlock.insertCountB); i++) {
                diffLines.add(new DiffLine(ChangeType.DELETED, diffResult.linesA[i + diffBlock.deleteStartA]));
                identical = false;
            }

            i = 0;
            for (; i < Math.min(diffBlock.deleteCountA, diffBlock.insertCountB); i++) {
                diffLines.add(new DiffLine(ChangeType.INSERTED, diffResult.linesB[i + diffBlock.insertStartB]));
                posB++;
                identical = false;
            }

            if (diffBlock.deleteCountA > diffBlock.insertCountB)
            {
                for (; i < diffBlock.deleteCountA; i++) {
                    diffLines.add(new DiffLine(ChangeType.DELETED, diffResult.linesA[i + diffBlock.deleteStartA]));
                    identical = false;
                }
            }
            else {
                for (; i < diffBlock.insertCountB; i++) {
                    diffLines.add(new DiffLine(ChangeType.INSERTED, diffResult.linesB[i + diffBlock.insertStartB]));
                    posB++;
                    identical = false;
                }
            }
        }

        for (; posB < diffResult.linesB.length; posB++) {
            diffLines.add(new DiffLine(ChangeType.UNCHANGED, diffResult.linesB[posB]));
        }

        return new CompareResult(diffLines, identical);
    }
    
    private DiffResult createLineDiffs(String textA, String textB)
    throws Exception
    {
        String[] sourceLinesA = textA.split(Utils.EOL);
        String[] sourceLinesB = textB.split(Utils.EOL);
        
        Map<String, Integer> lineHash = new HashMap<String, Integer>();

        int[] hashedLinesA = new int[sourceLinesA.length];
        int[] hashedLinesB = new int[sourceLinesB.length];

        buildLineHashes(lineHash, sourceLinesA, hashedLinesA);
        buildLineHashes(lineHash, sourceLinesB, hashedLinesB);

        boolean[] modificationsA = new boolean[sourceLinesA.length];
        boolean[] modificationsB = new boolean[sourceLinesB.length];

        buildModifications(
            hashedLinesA, modificationsA,
            hashedLinesB, modificationsB);

        List<DiffBlock> diffBlocks = new ArrayList<DiffBlock>();

        int nLinesA = hashedLinesA.length;
        int nLinesB = hashedLinesB.length;
        int posA = 0;
        int posB = 0;
        
        do {
        	while (posA < nLinesA && posB < nLinesB && !modificationsA[posA] && !modificationsB[posB]) {
                posA++; posB++;
        	}
            int beginA = posA;
            int beginB = posB;
            
            while (posA < nLinesA && modificationsA[posA]) ++posA;
            while (posB < nLinesB && modificationsB[posB]) ++posB;
            
            int deleteCount = posA - beginA;
            int insertCount = posB - beginB;

            if (deleteCount > 0 || insertCount > 0)
                diffBlocks.add(new DiffBlock(beginA, deleteCount, beginB, insertCount));
        }
        while (posA < nLinesA && posB < nLinesB);
    	
        DiffResult result = new DiffResult(sourceLinesA, sourceLinesB, diffBlocks);
        return result;
    }
    
    private void buildLineHashes(
        Map<String, Integer> lineHash,
        String[]             sourceLines,
        int[]                hashedLines)
    throws Exception
    {
        for (int i = 0; i < sourceLines.length; i++)
        {
            String line = sourceLines[i];

            if (m_ignoreWhiteSpace) line = line.trim();
            if (m_ignoreCase) line = line.toLowerCase();

            if (lineHash.containsKey(line)) {
                hashedLines[i] = lineHash.get(line);
            }
            else {
                hashedLines[i] = lineHash.size();
                lineHash.put(line, lineHash.size());
            }
        }
    }
    
    private void buildModifications(
        int[] hashedLinesA, boolean[] modificationsA,
        int[] hashedLinesB, boolean[] modificationsB)
    throws Exception
    {
        int n = hashedLinesA.length;
        int m = hashedLinesB.length;
        int max = m + n + 1;
        int[] forwardDiagonal = new int[max + 1];
        int[] reverseDiagonal = new int[max + 1];

        buildModificationData(
            hashedLinesA, modificationsA, 0, n,
            hashedLinesB, modificationsB, 0, m,
            forwardDiagonal, reverseDiagonal);
    }
    
    private void buildModificationData(
        int[] hashedLinesA, boolean[] modificationsA, int startA, int endA,
        int[] hashedLinesB, boolean[] modificationsB, int startB, int endB,
        int[] forwardDiagonal,
        int[] reverseDiagonal)
    throws Exception
    {
        while (startA < endA && startB < endB && hashedLinesA[startA] == hashedLinesB[startB]) {
            startA++; startB++;
        }
        while (startA < endA && startB < endB && hashedLinesA[endA - 1] == hashedLinesB[endB - 1]) {
            endA--; endB--;
        }
        
        int lengthA = endA - startA;
        int lengthB = endB - startB;
        
        if (lengthA > 0 && lengthB > 0)
        {
            EditLengthResult result = calculateEditLength(
                hashedLinesA, startA, endA,
                hashedLinesB, startB, endB,
                forwardDiagonal, reverseDiagonal);

            if (result.editLength <= 0)
                return;

            if (result.lastEdit == Edit.DELETE_RIGHT && result.startX - 1 > startA)
                modificationsA[--result.startX] = true;
            else if (result.lastEdit == Edit.INSERT_DOWN && result.startY - 1 > startB)
                modificationsB[--result.startY] = true;
            else if (result.lastEdit == Edit.DELETE_LEFT && result.endX < endA)
                modificationsA[result.endX++] = true;
            else if (result.lastEdit == Edit.INSERT_UP && result.endY < endB)
                modificationsB[result.endY++] = true;

            buildModificationData(
                hashedLinesA, modificationsA, startA, result.startX,
                hashedLinesB, modificationsB, startB, result.startY,
                forwardDiagonal, reverseDiagonal);

            buildModificationData(
                hashedLinesA, modificationsA, result.endX, endA,
                hashedLinesB, modificationsB, result.endY, endB,
                forwardDiagonal, reverseDiagonal);
        }
        else if (lengthA > 0) // && lengthB = 0
        {
            for (int i = startA; i < endA; i++)
                modificationsA[i] = true;
        }
        else if (lengthB > 0) // && lengthA = 0
        {
            for (int i = startB; i < endB; i++)
                modificationsB[i] = true;
        }
    }
    
    private EditLengthResult calculateEditLength(
        int[] hashedLinesA, int startA, int endA,
        int[] hashedLinesB, int startB, int endB,
        int[] forwardDiagonal,
        int[] reverseDiagonal)
    throws Exception
    {
        if (hashedLinesA.length == 0 && hashedLinesB.length == 0)
            return new EditLengthResult();
        
        int n = endA - startA;
        int m = endB - startB;
        int max = m + n + 1;
        int half = max / 2;
        int delta = n - m;
        boolean isDeltaEven = delta % 2 == 0;

        forwardDiagonal[1 + half] = 0;
        reverseDiagonal[1 + half] = n + 1;
        
        Edit lastEdit;
        
        for (int d = 0; d <= half; d++)
        {
            for (int k = -d; k <= d; k += 2)
            {
                int kIndex = k + half;
                int x;

                if (k == -d || (k != d && forwardDiagonal[kIndex - 1] < forwardDiagonal[kIndex + 1])) {
                    x = forwardDiagonal[kIndex + 1];
                    lastEdit = Edit.INSERT_DOWN;
                }
                else {
                    x = forwardDiagonal[kIndex - 1] + 1;
                    lastEdit = Edit.DELETE_RIGHT;
                }

                int y = x - k;
                int startX = x;
                int startY = y;

                while (x < n && y < m && hashedLinesA[x + startA] == hashedLinesB[y + startB]) {
                    x += 1; y += 1;
                }
                forwardDiagonal[kIndex] = x;
                
                if (!isDeltaEven && k - delta >= (-d + 1) && k - delta <= (d - 1))
                {
                    int revKIndex = (k - delta) + half;
                    int revX = reverseDiagonal[revKIndex];
                    int revY = revX - k;

                    if (revX <= x && revY <= y)
                    {
                        EditLengthResult result = new EditLengthResult();
                        result.editLength = 2 * d - 1;
                        result.startX = startX + startA;
                        result.startY = startY + startB;
                        result.endX = x + startA;
                        result.endY = y + startB;
                        result.lastEdit = lastEdit;
                        return result;
                    }
                }
            }

            for (int k = -d; k <= d; k += 2)
            {
                int kIndex = k + half;
                int x;

                if (k == -d || (k != d && reverseDiagonal[kIndex + 1] <= reverseDiagonal[kIndex - 1])) {
                    x = reverseDiagonal[kIndex + 1] - 1;
                    lastEdit = Edit.DELETE_LEFT;
                }
                else {
                    x = reverseDiagonal[kIndex - 1];
                    lastEdit = Edit.INSERT_UP;
                }

                int y = x - (k + delta);
                int endX = x;
                int endY = y;

                while (x > 0 && y > 0 && hashedLinesA[startA + x - 1] == hashedLinesB[startB + y - 1]) {
                    x -= 1; y -= 1;
                }
                reverseDiagonal[kIndex] = x;

                if (isDeltaEven && k + delta >= -d && k + delta <= d)
                {
                    int forKIndex = (k + delta) + half;
                    int forX = forwardDiagonal[forKIndex];
                    int forY = forX - (k + delta);

                    if (forX >= x && forY >= y)
                    {
                        EditLengthResult result = new EditLengthResult();
                        result.editLength = 2 * d;
                        result.startX = x + startA;
                        result.startY = y + startB;
                        result.endX = endX + startA;
                        result.endY = endY + startB;
                        result.lastEdit = lastEdit;
                        return result;
                    }
                }
            }
        }

        throw new Exception("XDiffer.calculateEditLength: Should never get here");
    }
}

