const languageNames: Record<string, string> = {
  ara: 'Arabic',
  ben: 'Bengali',
  deu: 'German',
  eng: 'English',
  fra: 'French',
  hin: 'Hindi',
  ind: 'Indonesian',
  ita: 'Italian',
  jpn: 'Japanese',
  kor: 'Korean',
  por: 'Portuguese',
  rus: 'Russian',
  spa: 'Spanish',
  tam: 'Tamil',
  tel: 'Telugu',
  tha: 'Thai',
  tur: 'Turkish',
  und: 'Unknown language',
};

export function displayLanguage(language?: string): string {
  const normalized = (language || 'und').toLowerCase().replace('_', '-');
  return languageNames[normalized.split('-', 1)[0]] || normalized.toUpperCase();
}
